/**
 * aggregatorTypes.ts — Shared types for the multi-aggregator routing engine.
 *
 * Defines the internal types used by the Termii and BulkSMS clients,
 * the aggregator router, and the dispatcher.
 */

import { AggregatorProvider, AggregatorSendResult, MessageIntent } from '../types';

// ─── Route Decision ────────────────────────────────────────────────────────

export interface RouteDecision {
  intent: MessageIntent;
  primaryProvider: AggregatorProvider;
  fallbackProvider?: AggregatorProvider;
  /** Reason for this routing decision */
  reason: string;
}

// ─── Provider Health State ─────────────────────────────────────────────────

export interface ProviderHealthState {
  provider: AggregatorProvider;
  /** Timestamps of failed requests in the current window */
  failureTimestamps: number[];
  /** Total requests in the current window */
  requestCount: number;
  /** Total response time sum for avg calculation */
  totalResponseTimeMs: number;
  /** Last time health was evaluated */
  lastCheckedAt: number;
  /** Whether the provider is currently considered available */
  isAvailable: boolean;
}

// ─── Aggregator HTTP Client Interface ──────────────────────────────────────

export interface AggregatorHttpClient {
  /** Provider identifier */
  provider: AggregatorProvider;

  /**
   * Send a single SMS through the aggregator's REST API.
   * Returns a normalized AggregatorSendResult.
   */
  sendSms(
    to: string,
    message: string,
    senderId: string
  ): Promise<AggregatorSendResult>;

  /**
   * Check account balance with the aggregator.
   * Returns balance in kobo (or -1 if unavailable).
   */
  getBalance(): Promise<number>;
}

// ─── API Request/Response Types ────────────────────────────────────────────

/** Termii send SMS request payload */
export interface TermiiSendRequest {
  api_key: string;
  to: string;
  from: string;
  sms: string;
  type: 'plain' | 'unicode';
  channel: 'generic' | 'dnd' | 'whatsapp';
  media?: {
    url: string;
    caption: string;
  };
}

/** Termii send SMS response */
export interface TermiiSendResponse {
  message_id?: string;
  message?: string;
  balance?: number;
  user?: string;
  code?: string;
  status?: string;
}

/** BulkSMS Nigeria send SMS request payload */
export interface BulkSmsSendRequest {
  api_token: string;
  from: string;
  to: string;
  body: string;
  dnd?: number; // 2 = avoid DND, 1 = route through DND
}

/** BulkSMS Nigeria API response (used for both send and auth) */
export interface BulkSmsApiResponse {
  data?: {
    message_id?: string | number;
    status?: string;
    cost?: number;
    balance?: number;
    token?: string;
  };
  message?: string;
  error?: string;
  token?: string;
  [key: string]: unknown;
}
