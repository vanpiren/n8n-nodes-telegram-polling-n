/* eslint-disable n8n-nodes-base/node-dirname-against-convention */
import { ITriggerFunctions } from 'n8n-core';
import { IDataObject, INodeType, INodeTypeDescription, ITriggerResponse, NodeOperationError } from 'n8n-workflow';
import { ApiResponse, Update } from 'typegram';

export class TelegramPollingTrigger implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Telegram Trigger (long polling) Trigger',
		name: 'telegramPollingTrigger',
		icon: 'file:telegram.svg',
		group: ['trigger'],
		version: 1,
		description: 'Starts the workflow on a Telegram update via long polling',
		defaults: {
			name: 'Telegram Trigger',
		},
		inputs: [],
		outputs: ['main'],
		credentials: [
			{
				name: 'telegramApi',
				required: true,
			},
		],
		properties: [
			{
				displayName: 'Updates',
				name: 'updates',
				type: 'multiOptions',
				options: [
					{
						name: '*',
						value: '*',
						description: 'All updates',
					},
					{
						name: 'Bot Chat Member Updated',
						value: 'my_chat_member',
						description:
							"Trigger on the bot's chat member status was updated in a chat. For private chats, this update is received only when the bot is blocked or unblocked by the user.",
					},
					{
						name: 'Callback Query',
						value: 'callback_query',
						description: 'Trigger on new incoming callback query',
					},
					{
						name: 'Channel Post',
						value: 'channel_post',
						description:
							'Trigger on new incoming channel post of any kind — text, photo, sticker, etc',
					},
					{
						name: 'Chat Join Request',
						value: 'chat_join_request',
						description:
							'Trigger on a request to join the chat has been sent. The bot must have the can_invite_users administrator right in the chat to receive these updates.',
					},
					{
						name: 'Chosen Inline Result',
						value: 'chosen_inline_result',
						description:
							'Trigger on the result of an inline query that was chosen by a user and sent to their chat partner',
					},
					{
						name: 'Edited Channel Post',
						value: 'edited_channel_post',
						description:
							'Trigger on new version of a channel post that is known to the bot and was edited',
					},
					{
						name: 'Edited Message',
						value: 'edited_message',
						description:
							'Trigger on new version of a channel post that is known to the bot and was edited',
					},
					{
						name: 'Inline Query',
						value: 'inline_query',
						description: 'Trigger on new incoming inline query',
					},
					{
						name: 'Message',
						value: 'message',
						description: 'Trigger on new incoming message of any kind — text, photo, sticker, etc',
					},
					{
						name: 'Poll',
						value: 'poll',
						description:
							'Trigger on new poll state. Bots receive only updates about stopped polls and polls, which are sent by the bot.',
					},
					{
						name: 'Poll Answer',
						value: 'poll_answer',
						description:
							'Trigger on new poll answer. Bots receive only updates about stopped polls and polls, which are sent by the bot.',
					},
					{
						name: 'Pre-Checkout Query',
						value: 'pre_checkout_query',
						description:
							'Trigger on new incoming pre-checkout query. Contains full information about checkout.',
					},
					{
						name: 'Shipping Query',
						value: 'shipping_query',
						description:
							'Trigger on new incoming shipping query. Only for invoices with flexible price.',
					},
					{
						name: 'User Chat Member Updated',
						value: 'chat_member',
						description:
							'Trigger on the user chat member status was updated in a chat. The bot must be an administrator in the chat and must explicitly specify “chat_member” in the list of allowed_updates to receive these updates.',
					},
				],
				required: true,
				default: [],
				description: 'The update types to listen to',
			},
			{
				displayName: 'Limit',
				name: 'limit',
				type: 'number',
				typeOptions: {
					minValue: 1,
				},
				default: 50,
				// eslint-disable-next-line n8n-nodes-base/node-param-description-wrong-for-limit
				description: 'Limit the number of messages to be polled',
			},
			{
				displayName: 'Timeout',
				name: 'timeout',
				type: 'number',
				typeOptions: {
					minValue: 0,
				},
				default: 60,
				description: 'Timeout (in seconds) for the polling request',
			},
		],
	};

	async trigger(this: ITriggerFunctions): Promise<ITriggerResponse> {
		const MAX_RETRY_DELAY_MS = 30_000;
		const BASE_RETRY_DELAY_MS = 1_000;
		const LONG_POLL_GRACE_SECONDS = 15;
		const MAX_BACKOFF_EXPONENT = 8;
		const NON_RETRYABLE_ERROR_DELAY_MS = 60_000;

		const credentials = await this.getCredentials('telegramApi');

		const limit = this.getNodeParameter('limit') as number;
		const timeout = this.getNodeParameter('timeout') as number;

		let allowedUpdates = this.getNodeParameter('updates') as string[];

		if (allowedUpdates.includes('*')) {
			allowedUpdates = [] as string[];
		}

		let isPolling = true;

		const abortController = new AbortController();

		type PollingError = {
			code?: string;
			message?: string;
			name?: string;
			response?: { status?: number };
		};

		const isAbortError = (error: PollingError) => {
			const errorMessage = error.message?.toLowerCase() ?? '';

			return error.name === 'AbortError' || error.code === 'ABORT_ERR' || errorMessage.includes('aborted');
		};
		const sleep = async (ms: number) => {
			if (!isPolling || ms <= 0) {
				return;
			}

			let remaining = ms;
			while (isPolling && remaining > 0) {
				const chunk = Math.min(remaining, 500);
				await new Promise<void>((resolve) => setTimeout(resolve, chunk));
				remaining -= chunk;
			}
		};

		const retryableErrorCodes = new Set([
			'ETIMEDOUT',
			'ECONNRESET',
			'ECONNREFUSED',
			'EHOSTUNREACH',
			'ENETUNREACH',
			'EAI_AGAIN',
			'ECONNABORTED',
		]);

		const shouldRetry = (error: PollingError) => {
			if (!isPolling) {
				return false;
			}

			if (error.code && retryableErrorCodes.has(error.code)) {
				return true;
			}

			const errorMessage = error.message?.toLowerCase() ?? '';
			if (errorMessage.includes('socket hang up')) {
				return true;
			}

			const status = error.response?.status;
			if (status !== undefined && (status === 429 || status >= 500)) {
				return true;
			}

			return false;
		};

		const getRetryDelay = (consecutiveErrors: number) => {
			const backoff = BASE_RETRY_DELAY_MS * 2 ** Math.min(consecutiveErrors, MAX_BACKOFF_EXPONENT);
			const jitterMultiplier = 0.8 + Math.random() * 0.4;

			return Math.min(MAX_RETRY_DELAY_MS, Math.floor(backoff * jitterMultiplier));
		};

		const startPolling = async () => {
			let offset = 0;
			let consecutiveErrors = 0;

			while (isPolling) {
				// try-catch to handle 409s that on >v1.0 bring down the entire instance
				try {
					const response = (await this.helpers.request({
						method: 'post',
						uri: `https://api.telegram.org/bot${credentials.accessToken}/getUpdates`,
						body: {
							offset,
							limit,
							timeout,
							allowed_updates: allowedUpdates,
						},
						json: true,
						timeout: (timeout + LONG_POLL_GRACE_SECONDS) * 1000,
						// Keep abort signal to stop in-flight long-poll requests during trigger shutdown
						signal: abortController.signal,
					})) as ApiResponse<Update[]>;

					consecutiveErrors = 0;

					if (!response.ok) {
						const statusCode = (response as unknown as IDataObject).error_code as number | undefined;
						const description = (response as unknown as IDataObject).description as string | undefined;

						if (statusCode !== undefined && (statusCode === 429 || statusCode >= 500)) {
							consecutiveErrors += 1;
							await sleep(getRetryDelay(consecutiveErrors));
							continue;
						}

						consecutiveErrors += 1;
						console.error(
							`Telegram getUpdates non-retryable response${statusCode ? ` ${statusCode}` : ''}${description ? `: ${description}` : ''}. Retrying in ${NON_RETRYABLE_ERROR_DELAY_MS}ms.`,
						);
						await sleep(NON_RETRYABLE_ERROR_DELAY_MS);
						continue;
					}

					if (!response.result) {
						continue;
					}

					let updates = response.result;
					if (updates.length > 0) {
						offset = updates[updates.length - 1].update_id + 1;

						if (allowedUpdates.length > 0) {
							updates = updates.filter((update) =>
								Object.keys(update).some((x) => allowedUpdates.includes(x)),
							);
						}

						this.emit([updates.map((update) => ({ json: update as unknown as IDataObject }))]);
					}
				} catch (error) {
					const pollingError = error as PollingError;

					if (!isPolling || isAbortError(pollingError)) {
						continue;
					}

					if (shouldRetry(pollingError)) {
						consecutiveErrors += 1;

						await sleep(getRetryDelay(consecutiveErrors));
						continue;
					}
					// Any other unexpected error: keep polling alive and retry with backoff
					consecutiveErrors += 1;
					console.error('Telegram polling unexpected error, retrying:', pollingError);
					await sleep(getRetryDelay(consecutiveErrors));
					continue;
				}
			}
		};

		startPolling().catch((error) => {
			if (isPolling) {
				console.error('Telegram polling failed and stopped:', error);
			}
		});

		const closeFunction = async () => {
			isPolling = false;
			abortController.abort();
		};

		return {
			closeFunction,
		};
	}
}
