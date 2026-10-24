/**
 * Shared TypeScript interfaces for the SMS Gateway Core.
 * Covers SMPP session state, message payloads, telco routing, and telemetry.
 */

// ─── Nigerian Telco Operators ───────────────────────────────────────────────

export enum TelcoOperator {
  MTN = 'mtn',
  AIRTEL = 'airtel',
  GLO = 'glo',
  NINEMOBILE = '9mobile',
  UNKNOWN = 'unknown',
}

// ─── SMPP Session State ─────────────────────────────────────────────────────

export enum SessionState {
  DISCONNECTED = 'disconnected',
  CONNECTING = 'connecting',
  BOUND_TRX = 'bound_trx',
  BOUND_TX = 'bound_tx',
  BOUND_RX = 'bound_rx',
  UNBOUND = 'unbound',
  CLOSED = 'closed',
  ERROR = 'error',
}

export interface SmppSessionConfig {
  host: string;
  port: number;
  systemId: string;
  password: string;
  systemType?: string;
  interfaceVersion?: number; // 0x34 (SMPP 3.4) or 0x50 (SMPP 5.0)
  ton?: number;
  npi?: number;
  addressRange?: string;
  sessionId: string;
  bindType: 'transceiver' | 'transmitter' | 'receiver';
  weight?: number; // For weighted load balancing
}

export interface SmppSessionMetrics {
  sessionId: string;
  state: SessionState;
  operator: TelcoOperator;
  connectedAt: number | null;
  lastActivityAt: number;
  messagesSubmitted: number;
  messagesDelivered: number;
  messagesFailed: number;
  errorsInWindow: number;
  avgResponseTimeMs: number;
  currentWindowUsage: number;
  maxWindowSize: number;
  isCircuitOpen: boolean;
}

// ─── Message Payload ────────────────────────────────────────────────────────

export interface IncomingMessage {
  id?: string;
  sender: string;       // source_addr / alphanumeric Sender ID
  recipient: string;    // MSISDN (e.g., +2348031234567)
  message: string;      // Message body
  priority?: 'otp' | 'bulk' | 'transactional';
  encoding?: 'gsm7' | 'ucs2' | 'auto';
  scheduleAt?: number;  // Unix timestamp for scheduled delivery
  campaignId?: string;
  userId?: string;
  apiKey?: string;
}

export interface ProcessedMessage extends IncomingMessage {
  operator: TelcoOperator;
  normalizedRecipient: string;   // E.164 normalized
  dataCoding: number;            // 0x00 (GSM7) | 0x08 (UCS2)
  segments: MessageSegment[];
  totalSegments: number;
  smppSessionId?: string;
}

export interface MessageSegment {
  sequenceNumber: number;
  totalParts: number;
  udh: Buffer | null;           // 6-byte UDH for concatenated SMS
  payload: Buffer;              // Encoded message content
  dataCoding: number;
  esmClass: number;             // 0x00 (normal) | 0x40 (UDH present)
}

// ─── Delivery Receipt ───────────────────────────────────────────────────────

export interface DeliveryReceipt {
  messageId: string;            // SMSC message ID
  recipient: string;
  status: DlrStatus;
  doneDate: string;
  submitDate: string;
  errorCode: string;
  rawPdu: string;
  receivedAt: number;
  operator: TelcoOperator;
  sessionId: string;
}

export enum DlrStatus {
  DELIVERED = 'DELIVERED',
  PENDING = 'PENDING',
  EXPIRED = 'EXPIRED',
  REJECTED = 'REJECTED',
  UNDELIVERABLE = 'UNDELIVERABLE',
  ACCEPTED = 'ACCEPTED',
  UNKNOWN = 'UNKNOWN',
}

// ─── Kafka Topics ───────────────────────────────────────────────────────────

export enum KafkaTopics {
  HIGH_PRIORITY_OTP = 'sms-high-priority-otp',
  LOW_PRIORITY_BULK = 'sms-low-priority-bulk',
  DLR_PROCESSING = 'sms-dlr-processing',
  SYSTEM_TELEMETRY = 'sms-system-telemetry',
}

// ─── Circuit Breaker ────────────────────────────────────────────────────────

export enum CircuitState {
  CLOSED = 'closed',       // Normal operation
  OPEN = 'open',           // Tripped — diverting traffic
  HALF_OPEN = 'half_open', // Testing if service recovered
}

export interface CircuitBreakerConfig {
  sessionId: string;
  errorThresholdPercent: number;  // e.g., 0.15 = 15%
  rollingWindowMs: number;       // e.g., 30000 (30 seconds)
  cooldownMs: number;            // Time before half-open probe
  minRequestCount: number;       // Minimum requests before tripping
}

// ─── Sender ID Rotation ─────────────────────────────────────────────────────

export interface SenderIdConfig {
  userId: string;
  activeSenderId: string;
  rotationPool: string[];
  successRateThreshold: number;  // e.g., 0.90 = 90%
  evaluationWindowMs: number;   // e.g., 300000 (5 minutes)
}

// ─── Admin Route Override ──────────────────────────────────────────────────

export enum RouteAction {
  ACTIVE = 'active',
  DRAIN = 'drain',
  DISABLED = 'disabled',
}

export interface RouteOverride {
  operator: TelcoOperator;
  action: RouteAction;
  priority: number;
  overriddenBy: string;    // Admin user ID
  overriddenAt: number;
  expiresAt?: number;
  fallbackRoute?: string;  // e.g., "international-fallback"
}

// ─── Aggregator Routing ────────────────────────────────────────────────────

export enum AggregatorProvider {
  TERMII = 'termii',
  BULKSMS = 'bulksms',
}

export enum MessageIntent {
  TRANSACTIONAL = 'transactional',  // OTP, alerts — fast lane
  PROMOTIONAL = 'promotional',      // marketing — cost-optimised
}

export interface AggregatorConfig {
  provider: AggregatorProvider;
  /** REST API base URL */
  baseUrl: string;
  /** Auth credentials */
  apiKey?: string;
  secretKey?: string;
  username?: string;
  password?: string;
  /** Default Sender ID for this provider */
  defaultSenderId: string;
  /** Cost per SMS segment in kobo */
  costPerSegment: number;
  /** Timeout for API calls (ms) */
  timeoutMs: number;
  /** Max retries per send */
  maxRetries: number;
}

export interface AggregatorSendResult {
  success: boolean;
  messageId?: string;
  provider: AggregatorProvider;
  cost: number;
  segments: number;
  responseTimeMs: number;
  error?: string;
}

export interface AggregatorRouteRule {
  intent: MessageIntent;
  primaryProvider: AggregatorProvider;
  fallbackProvider?: AggregatorProvider;
  /** If true, send through both and deduplicate downstream */
  dualSend?: boolean;
}

export interface AggregatorHealth {
  provider: AggregatorProvider;
  successRate: number;        // 0.0 to 1.0
  totalSent: number;
  totalFailed: number;
  avgResponseTimeMs: number;
  lastCheckedAt: number;
  isAvailable: boolean;
}

// ─── Telemetry ──────────────────────────────────────────────────────────────

export interface SystemTelemetry {
  timestamp: number;
  ingestionMps: number;
  deliveryMps: number;
  kafkaLagHighPriority: number;
  kafkaLagLowPriority: number;
  kafkaLagDlr: number;
  activeSessions: number;
  sessionsByOperator: Record<TelcoOperator, number>;
  errorRateByOperator: Record<TelcoOperator, number>;
  totalQueued: number;
  totalPendingDlr: number;
  redisMemoryUsage: number;
}