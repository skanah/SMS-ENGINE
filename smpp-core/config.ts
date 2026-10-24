/**
 * config.ts — Centralized Technical Constants & Configuration
 *
 * Contains all tunable parameters for the SMS Gateway Core:
 *   - Nigerian Telco SMPP bind configurations
 *   - Sliding window protocol parameters
 *   - Prefix → operator lookup matrix (all known Nigerian mobile prefixes)
 *   - Heartbeat, reconnect, and circuit breaker thresholds
 *   - GSM/UCS-2 encoding constants
 *   - Kafka topic names & Redis key schemas
 *
 * Loads overrides from process.env at startup.
 */

import dotenv from 'dotenv';
import { TelcoOperator, SmppSessionConfig } from './types';

dotenv.config();

// ─── Helper: parse int from env with fallback ─────────────────────────────
const intEnv = (key: string, fallback: number): number => {
  const val = process.env[key];
  if (val === undefined || val === '') return fallback;
  const parsed = parseInt(val, 10);
  return isNaN(parsed) ? fallback : parsed;
};

const strEnv = (key: string, fallback: string): string =>
  process.env[key] || fallback;

// ═══════════════════════════════════════════════════════════════════════════
// 1. SMPP SESSION CONFIGURATIONS — One factory per Nigerian telco
// ═══════════════════════════════════════════════════════════════════════════

export function buildTelcoSessionConfigs(): SmppSessionConfig[] {
  const maxSessions = intEnv('MAX_CONCURRENT_SESSIONS', 30);

  // Weighted distribution: MTN & Airtel get more connections (heavier traffic)
  const sessions: SmppSessionConfig[] = [];

  const telcos: Array<{
    key: string;
    operator: TelcoOperator;
    hostEnv: string;
    portEnv: string;
    idEnv: string;
    passEnv: string;
    weight: number;
  }> = [
    {
      key: TelcoOperator.MTN,
      operator: TelcoOperator.MTN,
      hostEnv: 'MTN_HOST',
      portEnv: 'MTN_PORT',
      idEnv: 'MTN_SYSTEM_ID',
      passEnv: 'MTN_PASSWORD',
      weight: 3,   // MTN ~40% market share
    },
    {
      key: TelcoOperator.AIRTEL,
      operator: TelcoOperator.AIRTEL,
      hostEnv: 'AIRTEL_HOST',
      portEnv: 'AIRTEL_PORT',
      idEnv: 'AIRTEL_SYSTEM_ID',
      passEnv: 'AIRTEL_PASSWORD',
      weight: 2,   // Airtel ~27%
    },
    {
      key: TelcoOperator.GLO,
      operator: TelcoOperator.GLO,
      hostEnv: 'GLO_HOST',
      portEnv: 'GLO_PORT',
      idEnv: 'GLO_SYSTEM_ID',
      passEnv: 'GLO_PASSWORD',
      weight: 2,   // Glo ~25%
    },
    {
      key: TelcoOperator.NINEMOBILE,
      operator: TelcoOperator.NINEMOBILE,
      hostEnv: 'NINEMOBILE_HOST',
      portEnv: 'NINEMOBILE_PORT',
      idEnv: 'NINEMOBILE_SYSTEM_ID',
      passEnv: 'NINEMOBILE_PASSWORD',
      weight: 1,   // 9mobile ~8%
    },
  ];

  // Calculate proportional session counts
  const totalWeight = telcos.reduce((sum, t) => sum + t.weight, 0);
  let allocated = 0;

  for (let i = 0; i < telcos.length; i++) {
    const t = telcos[i];
    // Last telco gets remainder to guarantee exact maxSessions total
    const count =
      i === telcos.length - 1
        ? maxSessions - allocated
        : Math.max(1, Math.round((t.weight / totalWeight) * maxSessions));

    for (let j = 0; j < count; j++) {
      const sessionId = `${t.key}-session-${j + 1}`;
      sessions.push({
        host: strEnv(t.hostEnv, `${t.key}.smpp.local`),
        port: intEnv(t.portEnv, 2775),
        systemId: strEnv(t.idEnv, `${t.key}_gateway`),
        password: strEnv(t.passEnv, 'change_me'),
        systemType: 'smpp',
        interfaceVersion: 0x34, // SMPP 3.4
        ton: 0x00,              // Unknown
        npi: 0x00,              // Unknown
        sessionId,
        bindType: 'transceiver',
        weight: t.weight,
      });
      allocated++;
    }
  }

  return sessions;
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. SLIDING WINDOW PROTOCOL PARAMETERS
// ═══════════════════════════════════════════════════════════════════════════

export const SLIDING_WINDOW = {
  /** Maximum outstanding submit_sm PDUs per session before blocking */
  MAX_WINDOW_SIZE: intEnv('SLIDING_WINDOW_SIZE', 128),

  /** Minimum window size to allow (for severely degraded links) */
  MIN_WINDOW_SIZE: 16,

  /** Time in ms to wait for a submit_sm_resp before marking as lost */
  RESPONSE_TIMEOUT_MS: 5000,

  /** How often to check for stalled/lost packets in the window */
  WINDOW_SWEEP_INTERVAL_MS: 1000,

  /** Maximum number of concurrent batches to process per worker tick */
  BATCH_SIZE_PER_TICK: 200,

  /** Initial credit count when window is fully open */
  INITIAL_CREDITS: 128,
} as const;

// ═══════════════════════════════════════════════════════════════════════════
// 3. HEARTBEAT & RECONNECTION
// ═══════════════════════════════════════════════════════════════════════════

export const CONNECTION = {
  /** Interval (ms) between enquire_link requests to keep session alive */
  ENQUIRE_LINK_INTERVAL_MS: intEnv('HEARTBEAT_INTERVAL_MS', 30_000),

  /** Initial delay before attempting reconnection (ms) */
  RECONNECT_DELAY_MS: intEnv('RECONNECT_DELAY_MS', 5_000),

  /** Maximum delay cap for exponential backoff (ms) */
  RECONNECT_MAX_DELAY_MS: intEnv('RECONNECT_MAX_DELAY_MS', 30_000),

  /** Backoff multiplier */
  RECONNECT_BACKOFF_MULTIPLIER: 2,

  /** Number of consecutive reconnect failures before escalating */
  MAX_RECONNECT_FAILURES: 10,
} as const;

// ═══════════════════════════════════════════════════════════════════════════
// 4. CIRCUIT BREAKER DEFAULTS
// ═══════════════════════════════════════════════════════════════════════════

export const CIRCUIT_BREAKER = {
  /** Error rate threshold (0.15 = 15%) — trip circuit if exceeded */
  ERROR_THRESHOLD: 0.15,

  /** Rolling window duration in milliseconds */
  ROLLING_WINDOW_MS: 30_000,

  /** Cooldown period before transitioning to HALF_OPEN (ms) */
  COOLDOWN_MS: 15_000,

  /** Minimum number of requests required before circuit can trip */
  MIN_REQUEST_COUNT: 50,
} as const;

// ═══════════════════════════════════════════════════════════════════════════
// 5. GSM / UCS-2 ENCODING CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════

export const GSM_ENCODING = {
  /** Maximum characters in a single GSM 7-bit message */
  GSM7_SINGLE_MAX: 160,

  /** Maximum characters per segment when concatenated (GSM 7-bit) */
  GSM7_CONCAT_MAX: 153,

  /** Maximum characters in a single UCS-2 message */
  UCS2_SINGLE_MAX: 70,

  /** Maximum characters per segment when concatenated (UCS-2) */
  UCS2_CONCAT_MAX: 67,

  /** Data coding value for GSM 7-bit default alphabet */
  DATA_CODING_GSM7: 0x00,

  /** Data coding value for UCS-2 (Unicode) */
  DATA_CODING_UCS2: 0x08,

  /** ESM class value indicating UDH (User Data Header) present */
  ESM_CLASS_UDH: 0x40,

  /** ESM class for normal (non-UDH) messages */
  ESM_CLASS_NORMAL: 0x00,

  /** UDH length in bytes (6 bytes for 8-bit reference number) */
  UDH_LENGTH: 6,

  /** UDH Information Element Identifier for concatenated SMS (8-bit) */
  UDH_IEI_CONCAT_8BIT: 0x00,

  /** UDH Information Element Identifier for concatenated SMS (16-bit) */
  UDH_IEI_CONCAT_16BIT: 0x08,

  /** UDH header length for concatenated messages (1 + 3)  */
  UDH_HEADER_LENGTH: 0x05,
} as const;

// ═══════════════════════════════════════════════════════════════════════════
// 6. NIGERIAN TELCO PREFIX MATRIX — Complete prefix → operator mapping
// ═══════════════════════════════════════════════════════════════════════════
//
// Format: Array of [prefix, operator] tuples sorted by prefix length DESC.
// The prefixMatcher builds a Trie from these for O(prefix-length) lookup.
//
// Sources: NCC (Nigerian Communications Commission) numbering plan.
// Includes all known active prefixes as of 2025–2026.

export const PREFIX_TABLE: Array<{ prefix: string; operator: TelcoOperator }> = [
  // ── MTN Nigeria (+23480x, +23481x, +23490x, +23496x) ──────────────────
  { prefix: '234803', operator: TelcoOperator.MTN },
  { prefix: '234806', operator: TelcoOperator.MTN },
  { prefix: '234810', operator: TelcoOperator.MTN },
  { prefix: '234813', operator: TelcoOperator.MTN },
  { prefix: '234814', operator: TelcoOperator.MTN },
  { prefix: '234816', operator: TelcoOperator.MTN },
  { prefix: '234903', operator: TelcoOperator.MTN },
  { prefix: '234906', operator: TelcoOperator.MTN },
  { prefix: '234909', operator: TelcoOperator.MTN },
  { prefix: '234912', operator: TelcoOperator.MTN },
  { prefix: '234913', operator: TelcoOperator.MTN },
  { prefix: '234914', operator: TelcoOperator.MTN },
  { prefix: '234916', operator: TelcoOperator.MTN },
  { prefix: '234918', operator: TelcoOperator.MTN },
  { prefix: '234919', operator: TelcoOperator.MTN },
  { prefix: '234962', operator: TelcoOperator.MTN },
  { prefix: '234963', operator: TelcoOperator.MTN },
  { prefix: '234964', operator: TelcoOperator.MTN },
  { prefix: '234965', operator: TelcoOperator.MTN },
  { prefix: '234966', operator: TelcoOperator.MTN },
  { prefix: '234967', operator: TelcoOperator.MTN },
  { prefix: '234968', operator: TelcoOperator.MTN },
  { prefix: '234969', operator: TelcoOperator.MTN },
  { prefix: '234980', operator: TelcoOperator.MTN },
  { prefix: '234981', operator: TelcoOperator.MTN },

  // ── Airtel Nigeria (+23480x, +23490x, +23491x, +23470x, +23492x) ──────
  { prefix: '234802', operator: TelcoOperator.AIRTEL },
  { prefix: '234808', operator: TelcoOperator.AIRTEL },
  { prefix: '234812', operator: TelcoOperator.AIRTEL },
  { prefix: '234701', operator: TelcoOperator.AIRTEL },
  { prefix: '234702', operator: TelcoOperator.AIRTEL },
  { prefix: '234703', operator: TelcoOperator.AIRTEL },
  { prefix: '234704', operator: TelcoOperator.AIRTEL },
  { prefix: '234705', operator: TelcoOperator.AIRTEL },
  { prefix: '234706', operator: TelcoOperator.AIRTEL },
  { prefix: '234707', operator: TelcoOperator.AIRTEL },
  { prefix: '234708', operator: TelcoOperator.AIRTEL },
  { prefix: '234709', operator: TelcoOperator.AIRTEL },
  { prefix: '234902', operator: TelcoOperator.AIRTEL },
  { prefix: '234904', operator: TelcoOperator.AIRTEL },
  { prefix: '234907', operator: TelcoOperator.AIRTEL },
  { prefix: '234912', operator: TelcoOperator.AIRTEL },
  { prefix: '234915', operator: TelcoOperator.AIRTEL },
  { prefix: '234917', operator: TelcoOperator.AIRTEL },
  { prefix: '234920', operator: TelcoOperator.AIRTEL },
  { prefix: '234921', operator: TelcoOperator.AIRTEL },
  { prefix: '234922', operator: TelcoOperator.AIRTEL },
  { prefix: '234923', operator: TelcoOperator.AIRTEL },
  { prefix: '234924', operator: TelcoOperator.AIRTEL },
  { prefix: '234925', operator: TelcoOperator.AIRTEL },
  { prefix: '234926', operator: TelcoOperator.AIRTEL },
  { prefix: '234927', operator: TelcoOperator.AIRTEL },
  { prefix: '234928', operator: TelcoOperator.AIRTEL },
  { prefix: '234929', operator: TelcoOperator.AIRTEL },

  // ── Glo Mobile (+23480x, +23490x, +23470x, +23481x, +23491x) ──────────
  { prefix: '234805', operator: TelcoOperator.GLO },
  { prefix: '234807', operator: TelcoOperator.GLO },
  { prefix: '234815', operator: TelcoOperator.GLO },
  { prefix: '234817', operator: TelcoOperator.GLO },
  { prefix: '234905', operator: TelcoOperator.GLO },
  { prefix: '234908', operator: TelcoOperator.GLO },
  { prefix: '234915', operator: TelcoOperator.GLO },
  { prefix: '234701', operator: TelcoOperator.GLO },
  { prefix: '234705', operator: TelcoOperator.GLO },
  { prefix: '234706', operator: TelcoOperator.GLO },

  // ── 9mobile (Etisalat) (+23480x, +23490x, +23481x, +23491x) ───────────
  { prefix: '234809', operator: TelcoOperator.NINEMOBILE },
  { prefix: '234818', operator: TelcoOperator.NINEMOBILE },
  { prefix: '234819', operator: TelcoOperator.NINEMOBILE },
  { prefix: '234901', operator: TelcoOperator.NINEMOBILE },
  { prefix: '234908', operator: TelcoOperator.NINEMOBILE },
  { prefix: '234909', operator: TelcoOperator.NINEMOBILE },
  { prefix: '234910', operator: TelcoOperator.NINEMOBILE },
  { prefix: '234911', operator: TelcoOperator.NINEMOBILE },
  { prefix: '234911', operator: TelcoOperator.NINEMOBILE },
];

// ═══════════════════════════════════════════════════════════════════════════
// 7. KAFKA TOPICS & REDIS KEYS
// ═══════════════════════════════════════════════════════════════════════════

export const KAFKA = {
  CLIENT_ID: strEnv('KAFKA_CLIENT_ID', 'sms-gateway-core'),
  BROKERS: (strEnv('KAFKA_BROKERS', 'localhost:9092')).split(',').map(s => s.trim()),
  TOPICS: {
    HIGH_PRIORITY_OTP: 'sms-high-priority-otp',
    LOW_PRIORITY_BULK: 'sms-low-priority-bulk',
    DLR_PROCESSING: 'sms-dlr-processing',
    SYSTEM_TELEMETRY: 'sms-system-telemetry',
  },
  /** Number of partitions per topic (should match consumer count) */
  PARTITIONS: 6,
  /** Number of consumers per consumer group */
  CONSUMERS: 3,
  /** Max batch size for DLR batch UPSERT consumer */
  DLR_BATCH_SIZE: 1000,
  /** Max wait time (ms) before flushing a DLR batch */
  DLR_BATCH_TIMEOUT_MS: 2000,
} as const;

export const REDIS_KEYS = {
  /** Prefix for network operator lookup cache (hash key) */
  PREFIX_CACHE: 'prefix:operator',

  /** DND registry set membership */
  DND_SET: 'dnd:numbers',

  /** User credit balance (string, integer) */
  USER_CREDIT: (userId: string) => `credit:${userId}`,

  /** Rate limit counter — sliding window */
  RATE_LIMIT: (userId: string, window: string) => `ratelimit:${userId}:${window}`,

  /** SMPP session metrics hash */
  SESSION_METRICS: (sessionId: string) => `session:${sessionId}:metrics`,

  /** Circuit breaker state */
  CIRCUIT_STATE: (sessionId: string) => `circuit:${sessionId}`,

  /** Route override from admin panel */
  ROUTE_OVERRIDE: (operator: string) => `route:override:${operator}`,

  /** Sender ID delivery success rate tracking */
  SENDER_ID_RATE: (senderId: string) => `senderid:${senderId}:rate`,

  /** Payment lock key (prevents double-credit on webhook retries) */
  PAYMENT_LOCK: (txRef: string) => `payment:lock:${txRef}`,
} as const;

// ═══════════════════════════════════════════════════════════════════════════
// 8. API & HTTP CONSTANTS
// ═══════════════════════════════════════════════════════════════════════════

export const API = {
  /** Fastify listen port */
  PORT: intEnv('API_PORT', 8080),
  /** Host binding */
  HOST: strEnv('API_HOST', '0.0.0.0'),
  /** Max payload size in bytes (10 MB for CSV uploads) */
  MAX_PAYLOAD_BYTES: 10 * 1024 * 1024,
  /** JWT secret */
  JWT_SECRET: strEnv('JWT_SECRET', 'change_me_jwt_secret_key_min_32_chars'),
  /** JWT token expiry */
  JWT_EXPIRY: strEnv('JWT_EXPIRY', '24h'),
  /** Rate limit: max requests per window per user */
  RATE_LIMIT_MAX: 100,
  /** Rate limit window in seconds */
  RATE_LIMIT_WINDOW_S: 1,
} as const;

// ═══════════════════════════════════════════════════════════════════════════
// 9. SENDER ID ROTATION
// ═══════════════════════════════════════════════════════════════════════════

export const SENDER_ID_ROTATION = {
  /** Minimum delivery success rate before triggering rotation */
  SUCCESS_RATE_THRESHOLD: 0.90,
  /** Evaluation window in milliseconds */
  EVALUATION_WINDOW_MS: 300_000, // 5 minutes
  /** Minimum messages in window before evaluating */
  MIN_MESSAGES_IN_WINDOW: 100,
} as const;

// ═══════════════════════════════════════════════════════════════════════════
// 10. DATABASE
// ═══════════════════════════════════════════════════════════════════════════

export const DATABASE = {
  URL: strEnv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/sms_gateway'),
  /** Max pool size for PG connection */
  POOL_SIZE: 20,
  /** Statement timeout in ms */
  STATEMENT_TIMEOUT: 30_000,
} as const;

// ═══════════════════════════════════════════════════════════════════════════
// 11. DEFAULT FALLBACK ROUTE
// ═══════════════════════════════════════════════════════════════════════════

export const FALLBACK_ROUTE = {
  /** When circuit is open, divert traffic here */
  HOST: 'smpp.international-fallback.com',
  PORT: 2775,
  SYSTEM_ID: 'fallback_gateway',
  PASSWORD: 'change_me',
} as const;

// ═══════════════════════════════════════════════════════════════════════════
// 12. AGGREGATOR ROUTING CONFIG — Termii (OTP) + BulkSMS Nigeria (Promo)
// ═══════════════════════════════════════════════════════════════════════════

export const AGGREGATORS = {
  TERMII: {
    API_KEY: strEnv('TERMII_API_KEY', ''),
    SECRET_KEY: strEnv('TERMII_SECRET_KEY', ''),
    BASE_URL: strEnv('TERMII_BASE_URL', 'https://api.termii.com'),
    SENDER_ID: strEnv('TERMII_SENDER_ID', 'SMSAlert'),
    TIMEOUT_MS: intEnv('TERMII_TIMEOUT_MS', 5000),
    MAX_RETRIES: intEnv('TERMII_MAX_RETRIES', 2),
    COST_PER_SEGMENT: intEnv('TERMII_COST_PER_SEGMENT', 65), // kobo
  },
  BULKSMS: {
    USERNAME: strEnv('BULKSMS_USERNAME', ''),
    PASSWORD: strEnv('BULKSMS_PASSWORD', ''),
    BASE_URL: strEnv('BULKSMS_BASE_URL', 'https://www.bulksmsnigeria.com/api/v1'),
    SENDER_ID: strEnv('BULKSMS_SENDER_ID', 'SMSNotify'),
    TIMEOUT_MS: intEnv('BULKSMS_TIMEOUT_MS', 10000),
    MAX_RETRIES: intEnv('BULKSMS_MAX_RETRIES', 3),
    COST_PER_SEGMENT: intEnv('BULKSMS_COST_PER_SEGMENT', 40), // kobo
  },
  ROUTE: {
    TRANSACTIONAL_PROVIDER: strEnv('ROUTE_TRANSACTIONAL_PROVIDER', 'termii') as 'termii' | 'bulksms',
    PROMOTIONAL_PROVIDER: strEnv('ROUTE_PROMOTIONAL_PROVIDER', 'bulksms') as 'termii' | 'bulksms',
    FALLBACK_PROVIDER: strEnv('ROUTE_FALLBACK_PROVIDER', 'termii') as 'termii' | 'bulksms',
    /** Rolling window for provider health tracking (ms) */
    HEALTH_WINDOW_MS: 300_000, // 5 minutes
    /** Error rate threshold before failing over */
    ERROR_THRESHOLD: 0.10, // 10%
    /** Minimum requests before evaluating health */
    MIN_REQUEST_COUNT: 20,
  },
} as const;

// ═══════════════════════════════════════════════════════════════════════════
// 13. REDIS CLIENT CONFIG — Supports both single-node and cluster topology
// ═══════════════════════════════════════════════════════════════════════════
//
// Use REDIS_URL for a connection string that auto-detects cluster mode:
//   Single node: redis://localhost:6379 or localhost:6379
//   Cluster:     redis://host1:6379,host2:6379,host3:6379
//   With auth:   redis://:password@host1:6379,host2:6379
//   TLS:         rediss://...
//
// LEGACY: individual REDIS_HOST/REDIS_PORT/REDIS_PASSWORD also supported.
// REDIS_URL takes precedence when set.

export const REDIS_CONFIG = {
  /** Full connection URL for auto-detecting single-node vs cluster */
  URL: strEnv('REDIS_URL', ''),
  /** Fallback host (used only if REDIS_URL is not set) */
  HOST: strEnv('REDIS_HOST', 'localhost'),
  /** Fallback port (used only if REDIS_URL is not set) */
  PORT: intEnv('REDIS_PORT', 6379),
  /** Fallback password (used only if REDIS_URL is not set) */
  PASSWORD: strEnv('REDIS_PASSWORD', ''),
  /** Key prefix to namespace all gateway keys */
  KEY_PREFIX: 'sms:',
  /** Default TTL for ephemeral keys (seconds) */
  DEFAULT_TTL_S: 3600,
  /** Max pool connections per Redis instance */
  MAX_POOL: 50,
  /** Retry delay on connection failure (ms) */
  RETRY_DELAY_MS: 1000,
  /** Max retry attempts before giving up */
  MAX_RETRIES: 10,
} as const;

/**
 * Build a Redis connection URL from individual config or return the
 * explicit REDIS_URL if it's set. This allows both configuration styles.
 */
export function resolveRedisUrl(): string {
  if (REDIS_CONFIG.URL) return REDIS_CONFIG.URL;
  const auth = REDIS_CONFIG.PASSWORD ? `:${REDIS_CONFIG.PASSWORD}@` : '';
  return `redis://${auth}${REDIS_CONFIG.HOST}:${REDIS_CONFIG.PORT}`;
}
