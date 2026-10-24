/**
 * redisClient.ts — Redis Client Factory with Single-Node & Cluster Support
 *
 * Parses the REDIS_URL environment variable to detect and support both
 * single-node (localhost:6379) and cluster topologies dynamically.
 *
 * REDIS_URL formats:
 *   - Single node:  redis://localhost:6379 or localhost:6379
 *   - Cluster:      redis://host1:6379,host2:6379,host3:6379
 *   - With auth:    redis://:password@host1:6379,host2:6379
 *
 * Exports a factory function that returns the appropriate Redis client
 * based on the parsed connection string.
 */

import Redis, { Redis as RedisClient, Cluster, ClusterOptions } from 'ioredis';
import { createLogger } from '../logger';

const log = createLogger('RedisClient');

// ─── Configuration ─────────────────────────────────────────────────────────

export interface RedisClientConfig {
  /** Full connection string or comma-separated list */
  url: string;
  /** Key prefix for namespacing */
  keyPrefix?: string;
  /** Default TTL for ephemeral keys (seconds) */
  defaultTtl?: number;
  /** Max retry attempts */
  maxRetries?: number;
  /** Retry delay on failure (ms) */
  retryDelayMs?: number;
}

// ─── Parsed Result ─────────────────────────────────────────────────────────

interface RedisNodeAddress {
  host: string;
  port: number;
}

interface ParsedRedisConfig {
  isCluster: boolean;
  nodes: RedisNodeAddress[];
  password?: string;
  tls?: boolean;
}

// ─── URL Parser ────────────────────────────────────────────────────────────

/**
 * Parse a REDIS_URL into node list and cluster detection.
 *
 * Supports:
 *   - redis://localhost:6379
 *   - redis://:password@host1:6379,host2:6379
 *   - localhost:6379
 *   - host1:6379,host2:6379,host3:6379
 */
function parseRedisUrl(url: string): ParsedRedisConfig {
  // Strip redis:// prefix if present
  let connectionString = url.trim();

  // Detect TLS
  const tls = connectionString.startsWith('rediss://');
  connectionString = connectionString.replace(/^rediss?:\/\//, '');

  // Extract password if present (format: :password@host)
  let password: string | undefined;
  const atIndex = connectionString.indexOf('@');
  if (atIndex !== -1) {
    const authPart = connectionString.slice(0, atIndex);
    if (authPart.startsWith(':')) {
      password = authPart.slice(1);
    }
    connectionString = connectionString.slice(atIndex + 1);
  }

  // Split by comma to get individual hosts
  const hostPorts = connectionString.split(',').map(s => s.trim()).filter(Boolean);

  const nodes: RedisNodeAddress[] = hostPorts.map(hp => {
    const [host, portStr] = hp.split(':');
    const port = portStr ? parseInt(portStr, 10) : 6379;
    return { host: host || 'localhost', port: isNaN(port) ? 6379 : port };
  });

  const isCluster = nodes.length > 1;

  return { isCluster, nodes, password, tls };
}

// ─── Factory ───────────────────────────────────────────────────────────────

/**
 * Create a Redis client (single-node or cluster) based on the connection URL.
 *
 * @param config - Redis client configuration
 * @returns Redis client instance (Redis for single-node, Cluster for multi-node)
 */
export function createRedisClient(config: RedisClientConfig): RedisClient | Cluster {
  const parsed = parseRedisUrl(config.url);

  const commonOptions = {
    keyPrefix: config.keyPrefix || 'sms:',
    retryStrategy: (times: number) => {
      const maxRetries = config.maxRetries ?? 10;
      if (times > maxRetries) {
        log.error(`Redis max retries (${maxRetries}) exceeded`);
        return null; // Stop retrying
      }
      const delay = Math.min((config.retryDelayMs ?? 1000) * Math.pow(2, times - 1), 30000);
      log.warn(`Redis reconnecting attempt ${times}/${maxRetries} in ${delay}ms`);
      return delay;
    },
    maxRetriesPerRequest: config.maxRetries ?? 10,
    enableReadyCheck: true,
    lazyConnect: true,
    showFriendlyErrorStack: process.env.NODE_ENV !== 'production',
  };

  if (parsed.isCluster) {
    const clusterOptions: ClusterOptions = {
      ...commonOptions,
      clusterRetryStrategy: commonOptions.retryStrategy,
      scaleReads: 'slave', // Distribute reads to replicas
      enableOfflineQueue: true,
      slotsRefreshTimeout: 10000,
    };

    if (parsed.password) {
      clusterOptions.redisOptions = { password: parsed.password };
    }
    if (parsed.tls) {
      clusterOptions.redisOptions = { ...clusterOptions.redisOptions, tls: {} };
    }

    const client = new Cluster(parsed.nodes as any, clusterOptions);
    log.info(`Redis Cluster client created: ${parsed.nodes.length} nodes`);
    return client;
  }

  // Single-node Redis
  const node: RedisNodeAddress = parsed.nodes[0] || { host: 'localhost', port: 6379 };
  const redisOptions: any = {
    ...commonOptions,
    host: node.host,
    port: node.port,
  };

  if (parsed.password) {
    redisOptions.password = parsed.password;
  }
  if (parsed.tls) {
    redisOptions.tls = {};
  }

  const client = new Redis(redisOptions);
  log.info(`Redis single-node client created: ${node.host}:${node.port}`);
  return client;
}

/**
 * Helper: check if a Redis client is a Cluster instance.
 */
export function isClusterClient(client: RedisClient | Cluster): client is Cluster {
  return (client as Cluster).nodes !== undefined;
}

/**
 * Helper: perform SISMEMBER on either single-node or cluster Redis.
 * For cluster, this uses the cluster's built-in hash slot routing.
 */
export async function redisSismember(
  client: RedisClient | Cluster,
  key: string,
  member: string
): Promise<boolean> {
  const result = await client.sismember(key, member);
  return result === 1;
}

/**
 * Helper: perform SMEMBERS on either single-node or cluster Redis.
 */
export async function redisSmembers(
  client: RedisClient | Cluster,
  key: string
): Promise<string[]> {
  return client.smembers(key);
}

/**
 * Helper: perform SADD on either single-node or cluster Redis.
 */
export async function redisSadd(
  client: RedisClient | Cluster,
  key: string,
  member: string
): Promise<number> {
  return client.sadd(key, member);
}

/**
 * Helper: perform SREM on either single-node or cluster Redis.
 */
export async function redisSrem(
  client: RedisClient | Cluster,
  key: string,
  member: string
): Promise<number> {
  return client.srem(key, member);
}

/**
 * Connect the Redis client and wait for ready state.
 */
export async function connectRedis(client: RedisClient | Cluster): Promise<void> {
  try {
    await client.connect();
    log.info('Redis client connected successfully');
  } catch (err: any) {
    log.error(`Redis connection failed: ${err.message}`);
    throw err;
  }
}

/**
 * Disconnect the Redis client gracefully.
 */
export async function disconnectRedis(client: RedisClient | Cluster): Promise<void> {
  try {
    await client.quit();
    log.info('Redis client disconnected');
  } catch (err: any) {
    log.warn(`Redis disconnect error: ${(err as Error).message}`);
    client.disconnect();
  }
}