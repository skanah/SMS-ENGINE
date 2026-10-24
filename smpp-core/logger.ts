/**
 * logger.ts — Structured Logging for SMS Gateway Core
 *
 * Lightweight wrapper around pino for high-performance logging.
 * Falls back to console if pino is not configured.
 */

import pino from 'pino';

// Configure pino with fastify-compatible formatting
const transport = pino.transport({
  target: 'pino/file',
  options: { destination: 1 }, // stdout
});

const rootLogger = pino(
  {
    level: process.env.LOG_LEVEL || 'info',
    timestamp: pino.stdTimeFunctions.isoTime,
    formatters: {
      level: (label) => ({ level: label }),
    },
    serializers: {
      err: pino.stdSerializers.err,
      error: pino.stdSerializers.err,
    },
  },
  transport
);

/**
 * Creates a child logger for a specific module.
 * Each module gets its own named logger instance.
 */
export function createLogger(module: string): pino.Logger {
  return rootLogger.child({ module });
}

export default rootLogger;