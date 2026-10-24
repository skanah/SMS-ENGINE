/**
 * termiiClient.ts — Termii REST API Client
 *
 * Termii is a Nigerian messaging aggregator with strong DND compliance
 * and fast OTP delivery. This client wraps their REST API for sending
 * SMS and checking balances.
 *
 * API Docs: https://developers.termii.com/
 * Endpoint: POST https://api.termii.com/api/sms/send
 */

import { AggregatorProvider, AggregatorSendResult } from '../types';
import { AGGREGATORS } from '../config';
import { createLogger } from '../logger';
import { AggregatorHttpClient, TermiiSendRequest, TermiiSendResponse } from './aggregatorTypes';

const log = createLogger('TermiiClient');

// ─── Termii Client ─────────────────────────────────────────────────────────

export class TermiiClient implements AggregatorHttpClient {
  readonly provider = AggregatorProvider.TERMII;
  private readonly apiKey: string;
  private readonly baseUrl: string;
  private readonly defaultSenderId: string;
  private readonly timeoutMs: number;
  private readonly maxRetries: number;

  constructor() {
    this.apiKey = AGGREGATORS.TERMII.API_KEY;
    this.baseUrl = AGGREGATORS.TERMII.BASE_URL;
    this.defaultSenderId = AGGREGATORS.TERMII.SENDER_ID;
    this.timeoutMs = AGGREGATORS.TERMII.TIMEOUT_MS;
    this.maxRetries = AGGREGATORS.TERMII.MAX_RETRIES;

    if (!this.apiKey) {
      log.warn('Termii API key not configured — client will return errors');
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

    if (!this.apiKey) {
      return {
        success: false,
        provider: AggregatorProvider.TERMII,
        cost: 0,
        segments: 1,
        responseTimeMs: Date.now() - startTime,
        error: 'Termii API key not configured',
      };
    }

    // Detect if message needs Unicode encoding
    const needsUnicode = /[^\x00-\x7F]/.test(message);
    const segments = needsUnicode
      ? Math.ceil(message.length / 67)
      : Math.ceil(message.length / 153);

    const payload: TermiiSendRequest = {
      api_key: this.apiKey,
      to,
      from: sender,
      sms: message,
      type: needsUnicode ? 'unicode' : 'plain',
      channel: 'generic', // Use generic channel for best delivery
    };

    let lastError: string | undefined;
    let attempts = 0;
    const maxAttempts = Math.max(1, this.maxRetries + 1);

    while (attempts < maxAttempts) {
      attempts++;
      try {
        const response = await this.httpPost(
          `${this.baseUrl}/api/sms/send`,
          payload
        );

        const elapsed = Date.now() - startTime;

        if (response.message_id) {
          log.info(
            `Termii send OK: msgId=${response.message_id} to=${to} ` +
            `segments=${segments} in ${elapsed}ms (attempt ${attempts})`
          );

          return {
            success: true,
            messageId: response.message_id,
            provider: AggregatorProvider.TERMII,
            cost: segments * AGGREGATORS.TERMII.COST_PER_SEGMENT,
            segments,
            responseTimeMs: elapsed,
          };
        }

        // Non-success response
        lastError = response.message || response.status || 'Unknown error';
        log.warn(
          `Termii attempt ${attempts}/${maxAttempts} failed: ${lastError}`
        );

        // Don't retry on auth errors
        if (response.code === 'AUTH_ERR' || response.code === 'FORBIDDEN') {
          break;
        }

        // Exponential backoff before retry
        if (attempts < maxAttempts) {
          await this.delay(Math.min(100 * Math.pow(2, attempts - 1), 2000));
        }
      } catch (err: any) {
        lastError = err.message || 'Network error';
        log.warn(
          `Termii attempt ${attempts}/${maxAttempts} network error: ${lastError}`
        );

        if (attempts < maxAttempts) {
          await this.delay(Math.min(200 * Math.pow(2, attempts - 1), 3000));
        }
      }
    }

    const elapsed = Date.now() - startTime;
    return {
      success: false,
      provider: AggregatorProvider.TERMII,
      cost: 0,
      segments,
      responseTimeMs: elapsed,
      error: lastError || 'Max retries exceeded',
    };
  }

  // ─── Get Balance ────────────────────────────────────────────────────────

  async getBalance(): Promise<number> {
    try {
      const response = await this.httpGet(
        `${this.baseUrl}/api/get-balance?api_key=${this.apiKey}`
      );
      return response.balance ?? -1;
    } catch (err: any) {
      log.error(`Termii balance check failed: ${err.message}`);
      return -1;
    }
  }

  // ─── HTTP Helpers ───────────────────────────────────────────────────────

  private async httpPost(url: string, body: any): Promise<TermiiSendResponse> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);

    try {
      const response = await fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(body),
        signal: controller.signal,
      });

      if (!response.ok) {
        const text = await response.text();
        throw new Error(`HTTP ${response.status}: ${text}`);
      }

      return await response.json() as TermiiSendResponse;
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