/**
 * aggregator/index.ts — Aggregator Routing Module Barrel Exports
 *
 * Exports all aggregator-related classes and types for easy importing.
 */

export { AggregatorRouter } from './aggregatorRouter';
export { AggregatorDispatcher } from './aggregatorDispatcher';
export { TermiiClient } from './termiiClient';
export { BulkSmsClient } from './bulksmsClient';
export type {
  RouteDecision,
  ProviderHealthState,
  AggregatorHttpClient,
  TermiiSendRequest,
  TermiiSendResponse,
  BulkSmsSendRequest,
  BulkSmsApiResponse,
} from './aggregatorTypes';