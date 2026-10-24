/**
 * bulksmsClient.ts — BulkSMS Nigeria REST API Client
 *
 * BulkSMS Nigeria is a cost-effective local aggregator ideal for
 * promotional and bulk messaging. This client wraps their REST API.
 *
 * API Docs: https://www.bulksmsnigeria.com/api/
 * Endpoint: POST https://www.bulksmsnigeria.com/api/v1/sms/create
 */

import { AggregatorProvider, AggregatorSendResult } from '../types';
import { AGGREGATORS } from '../config';
import { createLogger } from '../logger';
import { AggregatorHttpClient, BulkSmsSendRequest, BulkSmsApiResponse } from './aggregatorTypes';

const log = createLogger('BulkSmsClient');

// ─── BulkSMS Client ────────────────────────────────────────────────────────

export class BulkSmsClient implements AggregatorHttpClient {
  readonly provider = AggregatorProvider.BULKSMS;
  private readonly username: string;
  private readonly password: string;
  private readonly baseUrl: string;
  private readonly defaultSenderId: string;
  private readonly timeoutMs: number;
  private readonly maxRetries: number;
  private cachedToken: string | null = null;

  constructor() {
    this.username = AGGREGATORS.BULKSMS.USERNAME;
    this.password = AGGREGATORS.BULKSMS.PASSWORD;
    this.baseUrl = AGGREGATORS.BULKSMS.BASE_URL;
    this.defaultSenderId = AGGREGATORS.BULKSMS.SENDER_ID;
    this.timeoutMs = AGGREGATORS.BULKSMS.TIMEOUT_MS;
    this.maxRetries = AGGREGATORS.BULKSMS.MAX_RETRIES;

    if (!this.username || !this.password) {
      log.warn('BulkSMS credentials not configured — client will return errors');
    }
  }

  // ─── Send SMS ───────────────────────────────────────────────────────────

  async sendSms(
    to: string,
    message: string,
    senderId?: string
  ): Promise<AggregatorSendResult> {
    const startTime = Date.now();
    const sender = senderId || this.defaultSenderId;

    if (!this.username || !this.password) {
      return {
        success: false,
        provider: AggregatorProvider.BULKSMS,
        cost: 0,
        segments: 1,
        responseTimeMs: Date.now() - startTime,
        error: 'BulkSMS credentials not configured',
      };
    }

    // Get or reuse API token
    const token = await this.getToken();
    if (!token) {
      return {
        success: false,
        provider: AggregatorProvider.BULKSMS,
        cost: 0,
        segments: 1,
        responseTimeMs: Date.now() - startTime,
        error: 'Failed to authenticate with BulkSMS',
      };
    }

    const needsUnicode = /[^\x00-\x7F]/.test(message);
    const segments = needsUnicode
      ? Math.ceil(message.length / 67)
      : Math.ceil(message.length / 153);

    const payload: BulkSmsSendRequest = {
      api_token: token,
      from: sender,
      to,
      body: message,
      dnd: 2, // 2 = avoid DND numbers (for promotional)
    };

    let lastError: string | undefined;
    let attempts = 0;
    const maxAttempts = Math.max(1, this.maxRetries + 1);

    while (attempts < maxAttempts) {
      attempts++;
      try {
        const response = await this.httpPost(
          `${this.baseUrl}/sms/create`,
          payload
        );

        const elapsed = Date.now() - startTime;

        if (response.data?.message_id) {
          log.info(
            `BulkSMS send OK: msgId=${response.data.message_id} to=${to} ` +
            `segments=${segments} cost=${response.data.cost} in ${elapsed}ms`
          );

          return {
            success: true,
            messageId: String(response.data.message_id),
            provider: AggregatorProvider.BULKSMS,
            cost: segments * AGGREGATORS.BULKSMS.COST_PER_SEGMENT,
            segments,
            responseTimeMs: elapsed,
          };
        }

        // Non-success response
        lastError = response.error || response.message || 'Unknown error';
        log.warn(`BulkSMS attempt ${attempts}/${maxAttempts} failed: ${lastError}`);

        // Token expired — clear cache and retry
        if (response.message?.includes('token') || response.error?.includes('auth')) {
          this.cachedToken = null;
        }

        if (attempts < maxAttempts) {
          await this.delay(Math.min(100 * Math.pow(2, attempts - 1), 2000));
        }
      } catch (err: any) {
        lastError = err.message || 'Network error';
        log.warn(`BulkSMS attempt ${attempts}/${maxAttempts} error: ${lastError}`);

        if (attempts < maxAttempts) {
          await this.delay(Math.min(200 * Math.pow(2, attempts - 1), 3000));
        }
      }
    }

    const elapsed = Date.now() - startTime;
    return {
      success: false,
      provider: AggregatorProvider.BULKSMS,
      cost: 0,
      segments,
      responseTimeMs: elapsed,
      error: lastError || 'Max retries exceeded',
    };
  }

  // ─── Get Balance ────────────────────────────────────────────────────────

  async getBalance(): Promise<number> {
    try {
      const token = await this.getToken();
      if (!token) return -1;

      const response = await this.httpGet(
        `${this.baseUrl}/get-balance?api_token=${token}`
      );
      return response.data?.balance ?? -1;
    } catch (err: any) {
      log.error(`BulkSMS balance check failed: ${err.message}`);
      return -1;
    }
  }

  // ─── Authentication ─────────────────────────────────────────────────────

  /**
   * Get an API token from BulkSMS.
   * Tokens are cached in-memory and refreshed on expiry.
   */
  private async getToken(): Promise<string | null> {
    if (this.cachedToken) {
      return this.cachedToken;
    }

    try {
      const response = await this.httpPost(
        `${this.baseUrl}/auth/login` as any,
        {
          username: this.username,
          password: this.password,
        }
      );

      const token = response.data?.token || response.token;
      if (token) {
        this.cachedToken = token;
        log.info('BulkSMS authentication successful');
        return token;
      }

      log.error('BulkSMS auth failed: no token in response');
      return null;
    } catch (err: any) {
      log.error(`BulkSMS auth error: ${err.message}`);
      return null;
    }
  }

  // ─── HTTP Helpers ───────────────────────────────────────────────────────

  private async httpPost(url: string, body: any): Promise<BulkSmsApiResponse> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);

    try {
      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Accept': 'application/json',
        },
        body: JSON.stringify(body),
        signal: controller.signal,
      });

      if (!response.ok) {
        const text = await response.text();
        throw new Error(`HTTP ${response.status}: ${text}`);
      }

      return await response.json() as BulkSmsApiResponse;
    } finally {
      clearTimeout(timeout);
    }
  }

  private async httpGet(url: string): Promise<any> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);

    try {
      const response = await fetch(url, {
        method: 'GET',
        signal: controller.signal,
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      return await response.json();
    } finally {
      clearTimeout(timeout);
    }
  }

  private delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}