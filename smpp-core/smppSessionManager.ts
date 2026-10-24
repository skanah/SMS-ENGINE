/**
 * smppSessionManager.ts — SMPP Transceiver Session Pool Manager
 *
 * Manages a configurable pool of concurrent TRX (Transceiver) sessions
 * across multiple Nigerian telco gateways. Each session is an independent
 * TCP/SMPP binding with:
 *   - Persistent TRX binding with automatic reconnection (exponential backoff)
 *   - Configurable enquire_link heartbeat monitoring
 *   - Session health scoring (response times, error rates, uptime)
 *   - Graceful drain on shutdown
 *
 * Architecture:
 *   - One SmppSessionManager instance per worker thread
 *   - Each manager owns N sessions (configurable, default 25-30)
 *   - Sessions are load-balanced by weight (MTN gets more connections)
 *   - Health metrics are published to Redis for admin telemetry
 *
 * To hit 50,000 MPS with ~2,000 MPS per link, we need 25+ concurrent bindings.
 * This manager scales to 30+ sessions across 4 telcos.
 */

import * as smpp from 'smpp';
import { EventEmitter } from 'events';
import { createLogger } from './logger';
import {
  SmppSessionConfig,
  SmppSessionMetrics,
  SessionState,
  TelcoOperator,
} from './types';
import {
  buildTelcoSessionConfigs,
  CONNECTION,
  SLIDING_WINDOW,
} from './config';

// ─── Logger ─────────────────────────────────────────────────────────────────

const log = createLogger('SmppSessionManager');

// ─── Session Health Score ───────────────────────────────────────────────────

interface SessionHealth {
  sessionId: string;
  operator: TelcoOperator;
  state: SessionState;
  score: number;           // 0.0 (dead) to 1.0 (perfect)
  avgResponseTimeMs: number;
  errorRate: number;       // 0.0 to 1.0
  uptimePercent: number;   // 0.0 to 1.0
  lastError: string | null;
}

// ─── Internal Session Wrapper ───────────────────────────────────────────────

interface InternalSession {
  config: SmppSessionConfig;
  session: smpp.Session | null;
  state: SessionState;
  metrics: SmppSessionMetrics;
  heartbeatTimer: NodeJS.Timeout | null;
  reconnectTimer: NodeJS.Timeout | null;
  reconnectAttempts: number;
  sessionId: string;
  /** Resolve function for pending bind promise */
  bindResolve: ((value: boolean) => void) | null;
  /** Timestamp of last successful submit_sm_resp */
  lastResponseTime: number;
  /** Rolling array of response times for avg calculation */
  responseTimeSamples: number[];
  /** Error timestamps for rolling window error rate */
  errorTimestamps: number[];
}

// ─── Events ─────────────────────────────────────────────────────────────────

export interface SmppSessionEvents {
  'session:bound': (sessionId: string, operator: TelcoOperator) => void;
  'session:disconnected': (sessionId: string, operator: TelcoOperator, error?: Error) => void;
  'session:error': (sessionId: string, error: Error) => void;
  'session:health': (health: SessionHealth) => void;
  'submit_sm_resp': (sessionId: string, pdu: any) => void;
  'deliver_sm': (sessionId: string, pdu: any) => void;
  'enquire_link_resp': (sessionId: string, responseTimeMs: number) => void;
}

// ─── Session Manager ────────────────────────────────────────────────────────

export class SmppSessionManager extends EventEmitter {
  private sessions: Map<string, InternalSession> = new Map();
  private configs: SmppSessionConfig[];
  private isShuttingDown: boolean = false;
  private healthCheckInterval: NodeJS.Timeout | null = null;

  constructor() {
    super();
    this.configs = buildTelcoSessionConfigs();
    this.setMaxListeners(100); // Support many window processors
  }

  // ─── Initialization ───────────────────────────────────────────────────────

  /**
   * Initialize all SMPP sessions and begin binding.
   * Returns once all sessions have attempted to bind (some may fail).
   */
  async initialize(): Promise<void> {
    log.info(`Initializing ${this.configs.length} SMPP sessions...`);

    for (const config of this.configs) {
      this.createSession(config);
    }

    // Bind all sessions concurrently
    const bindPromises = Array.from(this.sessions.values()).map(s =>
      this.bindSession(s.sessionId)
    );

    await Promise.allSettled(bindPromises);

    const bound = this.getBoundCount();
    log.info(`Session initialization complete: ${bound}/${this.configs.length} bound`);

    // Start periodic health checks
    this.startHealthChecks();
  }

  // ─── Session Creation ─────────────────────────────────────────────────────

  private createSession(config: SmppSessionConfig): InternalSession {
    const internal: InternalSession = {
      config,
      session: null,
      sessionId: config.sessionId,
      state: SessionState.DISCONNECTED,
      metrics: {
        sessionId: config.sessionId,
        state: SessionState.DISCONNECTED,
        operator: this.getOperatorFromSessionId(config.sessionId),
        connectedAt: null,
        lastActivityAt: Date.now(),
        messagesSubmitted: 0,
        messagesDelivered: 0,
        messagesFailed: 0,
        errorsInWindow: 0,
        avgResponseTimeMs: 0,
        currentWindowUsage: 0,
        maxWindowSize: SLIDING_WINDOW.MAX_WINDOW_SIZE,
        isCircuitOpen: false,
      },
      heartbeatTimer: null,
      reconnectTimer: null,
      reconnectAttempts: 0,
      bindResolve: null,
      lastResponseTime: Date.now(),
      responseTimeSamples: [],
      errorTimestamps: [],
    };

    this.sessions.set(config.sessionId, internal);
    return internal;
  }

  // ─── Binding ──────────────────────────────────────────────────────────────

  /**
   * Bind a single session (TRX transceiver).
   * Returns true if bound successfully, false otherwise.
   */
  async bindSession(sessionId: string): Promise<boolean> {
    const internal = this.sessions.get(sessionId);
    if (!internal) {
      log.error(`Session ${sessionId} not found`);
      return false;
    }

    if (internal.state === SessionState.BOUND_TRX) {
      return true; // Already bound
    }

    return new Promise<boolean>(resolve => {
      internal.bindResolve = resolve;
      internal.state = SessionState.CONNECTING;

      const { config } = internal;
      log.info(`Binding session ${sessionId} to ${config.host}:${config.port}`);

      try {
        const session = smpp.connect({
          url: `smpp://${config.host}:${config.port}`,
          auto_enquire_link: false, // We manage heartbeats manually
          debug: false,
        });

        internal.session = session;

        session.on('connect', () => {
          log.debug(`Session ${sessionId} TCP connected, sending bind_transceiver...`);

          session.bind_transceiver(
            {
              system_id: config.systemId,
              password: config.password,
              system_type: config.systemType || 'smpp',
              interface_version: config.interfaceVersion || 0x34,
              addr_ton: config.ton || 0x00,
              addr_npi: config.npi || 0x00,
            },
            (pdu: any) => {
              if (pdu.command_status === 0) {
                internal.state = SessionState.BOUND_TRX;
                internal.metrics.state = SessionState.BOUND_TRX;
                internal.metrics.connectedAt = Date.now();
                internal.reconnectAttempts = 0;
                log.info(`Session ${sessionId} bound successfully (TRX)`);
                this.emit('session:bound', sessionId, internal.metrics.operator);
                this.startHeartbeat(sessionId);
                if (internal.bindResolve) {
                  internal.bindResolve(true);
                  internal.bindResolve = null;
                }
              } else {
                const errMsg = `Bind failed for ${sessionId}: command_status=${pdu.command_status}`;
                log.error(errMsg);
                internal.state = SessionState.ERROR;
                internal.metrics.state = SessionState.ERROR;
                if (internal.bindResolve) {
                  internal.bindResolve(false);
                  internal.bindResolve = null;
                }
                this.scheduleReconnect(sessionId);
              }
            }
          );
        });

        session.on('close', () => {
          log.warn(`Session ${sessionId} connection closed`);
          this.handleDisconnect(sessionId, new Error('Connection closed'));
        });

        session.on('error', (err: Error) => {
          log.error(`Session ${sessionId} socket error: ${err.message}`);
          this.handleDisconnect(sessionId, err);
        });

        // ─── PDU Handlers ────────────────────────────────────────────────

        session.on('pdu', (pdu: any) => {
          internal.metrics.lastActivityAt = Date.now();

          switch (pdu.command) {
            case 'submit_sm_resp':
              this.handleSubmitSmResp(sessionId, pdu);
              break;

            case 'deliver_sm':
              this.handleDeliverSm(sessionId, pdu);
              break;

            case 'enquire_link_resp':
              this.handleEnquireLinkResp(sessionId);
              break;

            case 'enquire_link':
              // Telco is pinging us — respond immediately
              session.send(
                pdu.response({
                  command: 'enquire_link_resp',
                  command_status: 0,
                })
              );
              break;

            case 'unbind':
              log.info(`Session ${sessionId} received unbind from telco`);
              session.send(
                pdu.response({
                  command: 'unbind_resp',
                  command_status: 0,
                })
              );
              this.handleDisconnect(sessionId, new Error('Unbind from telco'));
              break;
          }
        });

        // Set a connection timeout
        session.on('timeout', () => {
          log.warn(`Session ${sessionId} connection timeout`);
          session.close();
        });

      } catch (err: any) {
        log.error(`Failed to create session ${sessionId}: ${err.message}`);
        internal.state = SessionState.ERROR;
        if (internal.bindResolve) {
          internal.bindResolve(false);
          internal.bindResolve = null;
        }
        this.scheduleReconnect(sessionId);
      }
    });
  }

  // ─── PDU Handlers ─────────────────────────────────────────────────────────

  private handleSubmitSmResp(sessionId: string, pdu: any): void {
    const internal = this.sessions.get(sessionId);
    if (!internal) return;

    const responseTime = Date.now() - internal.lastResponseTime;
    internal.responseTimeSamples.push(responseTime);
    if (internal.responseTimeSamples.length > 100) {
      internal.responseTimeSamples.shift();
    }

    // Update average response time
    if (internal.responseTimeSamples.length > 0) {
      const sum = internal.responseTimeSamples.reduce((a, b) => a + b, 0);
      internal.metrics.avgResponseTimeMs = sum / internal.responseTimeSamples.length;
    }

    // Track delivery/failure
    if (pdu.command_status === 0) {
      internal.metrics.messagesDelivered++;
    } else {
      internal.metrics.messagesFailed++;
      this.recordError(sessionId);
    }

    // Decrement window usage (credit returned)
    internal.metrics.currentWindowUsage = Math.max(
      0,
      internal.metrics.currentWindowUsage - 1
    );

    this.emit('submit_sm_resp', sessionId, pdu);
  }

  private handleDeliverSm(sessionId: string, pdu: any): void {
    const internal = this.sessions.get(sessionId);
    if (!internal) return;

    // Immediately respond with success (0) to keep the channel open
    // The telco expects a quick ACK or they'll throttle/close the connection
    if (internal.session) {
      internal.session.send(
        pdu.response({
          command: 'deliver_sm_resp',
          command_status: 0,
        })
      );
    }

    // Emit the DLR for async processing (offloaded to Kafka in dlrHandler)
    this.emit('deliver_sm', sessionId, pdu);
  }

  private handleEnquireLinkResp(sessionId: string): void {
    const internal = this.sessions.get(sessionId);
    if (!internal) return;

    const responseTime = Date.now() - internal.lastResponseTime;
    this.emit('enquire_link_resp', sessionId, responseTime);
  }

  // ─── Heartbeat Management ─────────────────────────────────────────────────

  private startHeartbeat(sessionId: string): void {
    const internal = this.sessions.get(sessionId);
    if (!internal) return;

    this.stopHeartbeat(sessionId);

    internal.heartbeatTimer = setInterval(() => {
      if (internal.state !== SessionState.BOUND_TRX || !internal.session) {
        return;
      }

      internal.lastResponseTime = Date.now();

      internal.session.enquire_link(
        { sequence_number: Math.floor(Math.random() * 0x7FFFFFFF) },
        (pdu: any) => {
          if (pdu.command_status !== 0) {
            log.warn(`Session ${sessionId} enquire_link failed: status=${pdu.command_status}`);
            this.recordError(sessionId);
          }
        }
      );

      // If no response within 2x heartbeat interval, consider it dead
      const timeSinceLastActivity = Date.now() - internal.metrics.lastActivityAt;
      if (timeSinceLastActivity > CONNECTION.ENQUIRE_LINK_INTERVAL_MS * 2) {
        log.warn(`Session ${sessionId} heartbeat timeout, reconnecting...`);
        this.handleDisconnect(sessionId, new Error('Heartbeat timeout'));
      }
    }, CONNECTION.ENQUIRE_LINK_INTERVAL_MS);
  }

  private stopHeartbeat(sessionId: string): void {
    const internal = this.sessions.get(sessionId);
    if (internal && internal.heartbeatTimer) {
      clearInterval(internal.heartbeatTimer);
      internal.heartbeatTimer = null;
    }
  }

  // ─── Reconnection Logic ───────────────────────────────────────────────────

  private handleDisconnect(sessionId: string, error?: Error): void {
    const internal = this.sessions.get(sessionId);
    if (!internal) return;

    const previousState = internal.state;
    internal.state = SessionState.DISCONNECTED;
    internal.metrics.state = SessionState.DISCONNECTED;
    internal.metrics.connectedAt = null;

    this.stopHeartbeat(sessionId);

    // Clean up the session socket
    if (internal.session) {
      try {
        internal.session.close();
      } catch (_) {
        // Ignore close errors
      }
      internal.session = null;
    }

    if (error) {
      this.emit('session:disconnected', sessionId, internal.metrics.operator, error);
    }

    // Don't reconnect if shutting down
    if (this.isShuttingDown) return;

    this.scheduleReconnect(sessionId);
  }

  private scheduleReconnect(sessionId: string): void {
    const internal = this.sessions.get(sessionId);
    if (!internal) return;

    // Clear any existing reconnect timer
    if (internal.reconnectTimer) {
      clearTimeout(internal.reconnectTimer);
      internal.reconnectTimer = null;
    }

    internal.reconnectAttempts++;

    if (internal.reconnectAttempts > CONNECTION.MAX_RECONNECT_FAILURES) {
      log.error(`Session ${sessionId} exceeded max reconnect attempts (${CONNECTION.MAX_RECONNECT_FAILURES})`);
      internal.state = SessionState.ERROR;
      internal.metrics.state = SessionState.ERROR;
      return;
    }

    // Exponential backoff with cap
    const delay = Math.min(
      CONNECTION.RECONNECT_DELAY_MS *
        Math.pow(CONNECTION.RECONNECT_BACKOFF_MULTIPLIER, internal.reconnectAttempts - 1),
      CONNECTION.RECONNECT_MAX_DELAY_MS
    );

    log.info(`Scheduling reconnect for ${sessionId} in ${delay}ms (attempt ${internal.reconnectAttempts})`);

    internal.reconnectTimer = setTimeout(() => {
      if (this.isShuttingDown) return;
      internal.state = SessionState.CONNECTING;
      this.bindSession(sessionId);
    }, delay);
  }

  // ─── Error Tracking ───────────────────────────────────────────────────────

  private recordError(sessionId: string): void {
    const internal = this.sessions.get(sessionId);
    if (!internal) return;

    const now = Date.now();
    internal.errorTimestamps.push(now);
    internal.metrics.errorsInWindow = this.getErrorCountInWindow(sessionId);

    // Prune old errors outside the rolling window
    const cutoff = now - 30000; // 30-second window
    internal.errorTimestamps = internal.errorTimestamps.filter(t => t > cutoff);
  }

  /**
   * Returns the number of errors in the last 30-second rolling window.
   */
  getErrorCountInWindow(sessionId: string): number {
    const internal = this.sessions.get(sessionId);
    if (!internal) return 0;

    const cutoff = Date.now() - 30000;
    return internal.errorTimestamps.filter(t => t > cutoff).length;
  }

  /**
   * Returns the error rate (0.0 to 1.0) for a session in the rolling window.
   */
  getErrorRate(sessionId: string): number {
    const internal = this.sessions.get(sessionId);
    if (!internal) return 0;

    const totalRequests = internal.metrics.messagesSubmitted;
    if (totalRequests === 0) return 0;

    const errors = this.getErrorCountInWindow(sessionId);
    return errors / Math.max(totalRequests, 1);
  }

  // ─── Session Selection (Load Balancing) ───────────────────────────────────

  /**
   * Selects the healthiest available session for a given operator.
   * Uses weighted random selection biased by session health score.
   *
   * @param operator - Target telco operator
   * @returns The session ID of the selected session, or null if none available
   */
  selectSession(operator: TelcoOperator): string | null {
    const candidates = Array.from(this.sessions.values()).filter(s => {
      const op = this.getOperatorFromSessionId(s.config.sessionId);
      return (
        op === operator &&
        s.state === SessionState.BOUND_TRX &&
        !s.metrics.isCircuitOpen &&
        s.metrics.currentWindowUsage < s.metrics.maxWindowSize
      );
    });

    if (candidates.length === 0) {
      // Fallback: try any session for this operator even if circuit is open
      const fallback = Array.from(this.sessions.values()).find(s => {
        const op = this.getOperatorFromSessionId(s.config.sessionId);
        return op === operator && s.state === SessionState.BOUND_TRX;
      });
      return fallback?.config.sessionId || null;
    }

    // Weighted selection by health score
    const totalWeight = candidates.reduce(
      (sum, s) => sum + this.calculateHealthScore(s),
      0
    );

    if (totalWeight <= 0) {
      return candidates[0]?.config.sessionId || null;
    }

    let random = Math.random() * totalWeight;
    for (const candidate of candidates) {
      random -= this.calculateHealthScore(candidate);
      if (random <= 0) {
        return candidate.config.sessionId;
      }
    }

    return candidates[candidates.length - 1]?.config.sessionId || null;
  }

  /**
   * Calculate a health score (0.0 to 1.0) for a session.
   * Used for weighted load balancing.
   */
  private calculateHealthScore(internal: InternalSession): number {
    if (internal.state !== SessionState.BOUND_TRX) return 0;

    const windowUsageRatio =
      internal.metrics.maxWindowSize > 0
        ? 1 - internal.metrics.currentWindowUsage / internal.metrics.maxWindowSize
        : 0;

    const responseTimeScore =
      internal.metrics.avgResponseTimeMs > 0
        ? Math.max(0, 1 - internal.metrics.avgResponseTimeMs / 1000)
        : 1;

    const errorScore = 1 - this.getErrorRate(internal.config.sessionId);

    // Weighted combination
    return (
      windowUsageRatio * 0.4 +  // 40% weight: available window capacity
      responseTimeScore * 0.3 + // 30% weight: response speed
      errorScore * 0.3          // 30% weight: reliability
    );
  }

  // ─── Submit SM (Called by SlidingWindowProcessor) ─────────────────────────

  /**
   * Send a submit_sm PDU through a specific session.
   * This is a fire-and-forget call — the sliding window processor manages
   * the credit tracking and response handling.
   *
   * @param sessionId - The session to send through
   * @param params - The submit_sm PDU parameters
   * @returns The sequence number of the sent PDU, or -1 if send failed
   */
  sendSubmitSm(sessionId: string, params: any): number {
    const internal = this.sessions.get(sessionId);
    if (!internal || !internal.session || internal.state !== SessionState.BOUND_TRX) {
      return -1;
    }

    internal.lastResponseTime = Date.now();
    internal.metrics.messagesSubmitted++;
    internal.metrics.currentWindowUsage++;

    const sequenceNumber = Math.floor(Math.random() * 0x7FFFFFFF);

    internal.session.submit_sm(
      {
        source_addr: params.source_addr,
        destination_addr: params.destination_addr,
        short_message: params.short_message,
        data_coding: params.data_coding,
        esm_class: params.esm_class,
        registered_delivery: params.registered_delivery ?? 1,
        priority_flag: params.priority_flag ?? 0,
        validity_period: params.validity_period,
        schedule_delivery_time: params.schedule_delivery_time,
        sequence_number: sequenceNumber,
      },
      (pdu: any) => {
        // The PDU handler will process this response
        // But we need to ensure it's routed correctly
        this.handleSubmitSmResp(sessionId, pdu);
      }
    );

    return sequenceNumber;
  }

  // ─── Health Checks ────────────────────────────────────────────────────────

  private startHealthChecks(): void {
    this.healthCheckInterval = setInterval(() => {
      for (const [sessionId, internal] of this.sessions) {
        const health: SessionHealth = {
          sessionId,
          operator: internal.metrics.operator,
          state: internal.state,
          score: this.calculateHealthScore(internal),
          avgResponseTimeMs: internal.metrics.avgResponseTimeMs,
          errorRate: this.getErrorRate(sessionId),
          uptimePercent: this.calculateUptime(internal),
          lastError: null,
        };
        this.emit('session:health', health);
      }
    }, 5000); // Every 5 seconds
  }

  private calculateUptime(internal: InternalSession): number {
    if (!internal.metrics.connectedAt) return 0;
    const uptime = Date.now() - internal.metrics.connectedAt;
    const totalTime = uptime + internal.reconnectAttempts * CONNECTION.RECONNECT_DELAY_MS;
    if (totalTime === 0) return 0;
    return uptime / totalTime;
  }

  // ─── Utility Methods ──────────────────────────────────────────────────────

  private getOperatorFromSessionId(sessionId: string): TelcoOperator {
    if (sessionId.startsWith('mtn')) return TelcoOperator.MTN;
    if (sessionId.startsWith('airtel')) return TelcoOperator.AIRTEL;
    if (sessionId.startsWith('glo')) return TelcoOperator.GLO;
    if (sessionId.startsWith('9mobile')) return TelcoOperator.NINEMOBILE;
    return TelcoOperator.UNKNOWN;
  }

  getSession(sessionId: string): SmppSessionMetrics | null {
    const internal = this.sessions.get(sessionId);
    if (!internal) return null;
    return { ...internal.metrics };
  }

  getAllSessionMetrics(): SmppSessionMetrics[] {
    return Array.from(this.sessions.values()).map(s => ({ ...s.metrics }));
  }

  getBoundCount(): number {
    let count = 0;
    for (const s of this.sessions.values()) {
      if (s.state === SessionState.BOUND_TRX) count++;
    }
    return count;
  }

  getTotalWindowUsage(): number {
    let total = 0;
    for (const s of this.sessions.values()) {
      total += s.metrics.currentWindowUsage;
    }
    return total;
  }

  /**
   * Mark a session's circuit as open (called by CircuitBreaker).
   */
  setCircuitOpen(sessionId: string, open: boolean): void {
    const internal = this.sessions.get(sessionId);
    if (internal) {
      internal.metrics.isCircuitOpen = open;
    }
  }

  /**
   * Update the window usage for a session (called by SlidingWindowProcessor).
   */
  setWindowUsage(sessionId: string, usage: number): void {
    const internal = this.sessions.get(sessionId);
    if (internal) {
      internal.metrics.currentWindowUsage = Math.max(0, Math.min(usage, internal.metrics.maxWindowSize));
    }
  }

  // ─── Graceful Shutdown ────────────────────────────────────────────────────

  async shutdown(): Promise<void> {
    log.info('Shutting down SMPP session manager...');
    this.isShuttingDown = true;

    // Stop health checks
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
      this.healthCheckInterval = null;
    }

    // Unbind all sessions gracefully
    const unbindPromises = Array.from(this.sessions.entries()).map(
      ([sessionId, internal]) => {
        return new Promise<void>(resolve => {
          this.stopHeartbeat(sessionId);

          if (internal.reconnectTimer) {
            clearTimeout(internal.reconnectTimer);
            internal.reconnectTimer = null;
          }

          if (internal.session && internal.state === SessionState.BOUND_TRX) {
            internal.session.unbind((pdu: any) => {
              internal.session!.close();
              internal.state = SessionState.UNBOUND;
              internal.metrics.state = SessionState.UNBOUND;
              resolve();
            });

            // Force close after 2 seconds if unbind doesn't respond
            setTimeout(() => {
              try {
                internal.session?.close();
              } catch (_) {}
              resolve();
            }, 2000);
          } else {
            if (internal.session) {
              try {
                internal.session.close();
              } catch (_) {}
            }
            resolve();
          }
        });
      }
    );

    await Promise.allSettled(unbindPromises);
    this.sessions.clear();
    log.info('SMPP session manager shutdown complete');
  }
}
