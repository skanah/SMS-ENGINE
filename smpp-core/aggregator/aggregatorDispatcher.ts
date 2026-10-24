/**
 * aggregatorDispatcher.ts — Unified Aggregator Dispatch Engine
 *
 * Executes SMS delivery through the selected aggregator with:
 *   1. Primary provider dispatch
 *   2. Automatic failover on failure (for transactional messages)
 *   3. Timing and cost tracking
 *   4. Health state updates
 *
 * The dispatcher is the bridge between the route decision and
 * the actual HTTP call to the aggregator.
 */

import { AggregatorProvider, AggregatorSendResult, MessageIntent, IncomingMessage } from '../types';
import { createLogger } from '../logger';
import { RouteDecision, AggregatorHttpClient } from './aggregatorTypes';
import { AggregatorRouter } from './aggregatorRouter';
import { processMessage } from '../gsmProcessor';

const log = createLogger('AggregatorDispatcher');

// ─── Dispatcher ───────────────────────────────────────────────────────────

export class AggregatorDispatcher {
  private clients: Map<AggregatorProvider, AggregatorHttpClient> = new Map();
  private router: AggregatorRouter;

  constructor(router: AggregatorRouter) {
    this.router = router;
  }

  /**
   * Register an aggregator HTTP client that the dispatcher can use.
   */
  registerClient(client: AggregatorHttpClient): void {
    this.clients.set(client.provider, client);
  }

  /**
   * Send a message through the optimal aggregator based on its intent.
   *
   * Flow:
   *   1. Get route decision from router (primary + fallback)
   *   2. Try primary provider
   *   3. If primary succeeds → return result
   *   4. If primary fails AND transactional → try fallback
   *   5. If primary fails AND promotional → return failure (no failover for promo)
   *
   * @param msg - The incoming message to send
   * @param intent - Transactional (OTP) or Promotional (bulk)
   * @returns AggregatorSendResult with delivery status
   */
  async dispatch(
    msg: IncomingMessage,
    intent: MessageIntent
  ): Promise<AggregatorSendResult> {
    // 1. Get route decision
    const route = await this.router.getRoute(intent);

    // 2. Try primary provider
    const primaryResult = await this.sendViaProvider(
      route.primaryProvider,
      msg
    );

    // Track health
    if (primaryResult.success) {
      this.router.recordSuccess(route.primaryProvider, primaryResult.responseTimeMs);
      return primaryResult;
    }

    this.router.recordFailure(route.primaryProvider);

    // 3. For transactional (OTP), try fallback if available
    if (intent === MessageIntent.TRANSACTIONAL && route.fallbackProvider) {
      log.warn(
        `Primary ${route.primaryProvider} failed, ` +
        `failing over to ${route.fallbackProvider} for OTP`
      );

      const fallbackResult = await this.sendViaProvider(
        route.fallbackProvider,
        msg
      );

      if (fallbackResult.success) {
        this.router.recordSuccess(route.fallbackProvider, fallbackResult.responseTimeMs);
      } else {
        this.router.recordFailure(route.fallbackProvider);
      }

      return fallbackResult;
    }

    // 4. Promotional: don't failover, just return the primary error
    return primaryResult;
  }

  /**
   * Send a message through a specific provider.
   */
  private async sendViaProvider(
    provider: AggregatorProvider,
    msg: IncomingMessage
  ): Promise<AggregatorSendResult> {
    const client = this.clients.get(provider);
    if (!client) {
      return {
        success: false,
        provider,
        cost: 0,
        segments: 1,
        responseTimeMs: 0,
        error: `No client registered for provider: ${provider}`,
      };
    }

    try {
      const result = await client.sendSms(
        msg.recipient,
        msg.message,
        msg.sender
      );

      return result;
    } catch (err: any) {
      return {
        success: false,
        provider,
        cost: 0,
        segments: 1,
        responseTimeMs: 0,
        error: `Dispatch error: ${err.message}`,
      };
    }
  }

  /**
   * Calculate SMS segments for cost estimation.
   */
  estimateSegments(message: string, isUcs2: boolean): number {
    if (isUcs2) {
      return message.length <= 70 ? 1 : Math.ceil(message.length / 67);
    }
    return message.length <= 160 ? 1 : Math.ceil(message.length / 153);
  }

  /**
   * Calculate the estimated cost for a message through a given intent.
   */
  estimateCost(message: string, intent: MessageIntent): number {
    const isUcs2 = /[^\x00-\x7F]/.test(message);
    const segments = this.estimateSegments(message, isUcs2);

    const provider = intent === MessageIntent.TRANSACTIONAL
      ? 'TERMII' as const
      : 'BULKSMS' as const;

    const { AGGREGATORS } = require('../config');
    return segments * AGGREGATORS[provider].COST_PER_SEGMENT;
  }
}