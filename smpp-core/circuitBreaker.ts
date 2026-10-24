/**
 * circuitBreaker.ts — Rolling Window Error Rate Circuit Breaker
 *
 * Wraps each SMPP session with a circuit breaker that monitors error rates
 * over a rolling 30-second window. If the error rate exceeds 15% (configurable),
 * the circuit opens and traffic is diverted to a fallback route.
 *
 * States:
 *   CLOSED  → Normal operation, traffic flows through this session
 *   OPEN    → Tripped, all traffic diverted. No requests sent.
 *   HALF_OPEN → After cooldown, a probe request is sent to test recovery
 *
 * The circuit breaker is per-session, allowing fine-grained control per
 * telco binding. If one MTN link fails, only that link is drained — others
 * continue operating.
 */

import { EventEmitter } from 'events';
import { CircuitState } from './types';
import { CIRCUIT_BREAKER } from './config';
import { createLogger } from './logger';
import { SmppSessionManager } from './smppSessionManager';

const log = createLogger('CircuitBreaker');

// ─── Events ─────────────────────────────────────────────────────────────────

export interface CircuitBreakerEvents {
  'circuit:open': (sessionId: string, errorRate: number) => void;
  'circuit:closed': (sessionId: string) => void;
  'circuit:half_open': (sessionId: string) => void;
  'circuit:trip': (sessionId: string, errorRate: number, window: number) => void;
}

// ─── Circuit State Tracking ─────────────────────────────────────────────────

interface CircuitStateInternal {
  sessionId: string;
  state: CircuitState;
  /** Timestamps of errors in the current rolling window */
  errorTimestamps: number[];
  /** Total requests in the current window */
  requestCount: number;
  /** When the circuit was last opened */
  lastOpenedAt: number | null;
  /** When the circuit transitioned to half-open */
  halfOpenAt: number | null;
}

// ─── Circuit Breaker ───────────────────────────────────────────────────────

export class CircuitBreaker extends EventEmitter {
  private circuits: Map<string, CircuitStateInternal> = new Map();
  private sessionManager: SmppSessionManager;
  private sweepTimer: NodeJS.Timeout | null = null;

  constructor(sessionManager: SmppSessionManager) {
    super();
    this.sessionManager = sessionManager;
    this.setMaxListeners(100);

    // Initialize circuits for all sessions
    this.initializeCircuits();

    // Start periodic sweep to evaluate error rates and transition states
    this.startSweep();
  }

  // ─── Initialization ───────────────────────────────────────────────────────

  private initializeCircuits(): void {
    const metrics = this.sessionManager.getAllSessionMetrics();
    for (const m of metrics) {
      this.circuits.set(m.sessionId, {
        sessionId: m.sessionId,
        state: CircuitState.CLOSED,
        errorTimestamps: [],
        requestCount: 0,
        lastOpenedAt: null,
        halfOpenAt: null,
      });
    }
    log.info(`Initialized ${metrics.length} circuit breakers`);
  }

  // ─── Core Logic ───────────────────────────────────────────────────────────

  /**
   * Record a successful request for a session.
   * Call this when a submit_sm_resp comes back with command_status=0.
   */
  recordSuccess(sessionId: string): void {
    const circuit = this.circuits.get(sessionId);
    if (!circuit) return;

    circuit.requestCount++;

    // If we're half-open and get a success, close the circuit
    if (circuit.state === CircuitState.HALF_OPEN) {
      this.closeCircuit(sessionId);
    }
  }

  /**
   * Record a failed request for a session.
   * Call this when a submit_sm_resp comes back with a non-zero status,
   * or a socket error occurs.
   */
  recordFailure(sessionId: string): void {
    const circuit = this.circuits.get(sessionId);
    if (!circuit) return;

    const now = Date.now();
    circuit.errorTimestamps.push(now);
    circuit.requestCount++;
  }

  /**
   * Get the current error rate for a session over the rolling window.
   * Returns 0.0 to 1.0.
   */
  getErrorRate(sessionId: string): number {
    const circuit = this.circuits.get(sessionId);
    if (!circuit) return 0;

    const now = Date.now();
    const cutoff = now - CIRCUIT_BREAKER.ROLLING_WINDOW_MS;

    // Prune old errors
    circuit.errorTimestamps = circuit.errorTimestamps.filter(t => t > cutoff);

    if (circuit.requestCount < CIRCUIT_BREAKER.MIN_REQUEST_COUNT) {
      return 0; // Not enough data to trip
    }

    return circuit.errorTimestamps.length / Math.max(circuit.requestCount, 1);
  }

  /**
   * Get the current state of a session's circuit breaker.
   */
  getState(sessionId: string): CircuitState {
    return this.circuits.get(sessionId)?.state ?? CircuitState.CLOSED;
  }

  /**
   * Check if a session's circuit is open (should not send traffic).
   */
  isCircuitOpen(sessionId: string): boolean {
    const circuit = this.circuits.get(sessionId);
    if (!circuit) return false;
    return circuit.state === CircuitState.OPEN;
  }

  // ─── State Transitions ────────────────────────────────────────────────────

  private openCircuit(sessionId: string): void {
    const circuit = this.circuits.get(sessionId);
    if (!circuit || circuit.state === CircuitState.OPEN) return;

    circuit.state = CircuitState.OPEN;
    circuit.lastOpenedAt = Date.now();

    // Notify the session manager to mark this session as circuit open
    this.sessionManager.setCircuitOpen(sessionId, true);

    log.warn(`Circuit OPEN for session ${sessionId}`);
    this.emit('circuit:open', sessionId, this.getErrorRate(sessionId));
  }

  private closeCircuit(sessionId: string): void {
    const circuit = this.circuits.get(sessionId);
    if (!circuit) return;

    circuit.state = CircuitState.CLOSED;
    circuit.errorTimestamps = [];
    circuit.requestCount = 0;
    circuit.halfOpenAt = null;

    // Notify the session manager
    this.sessionManager.setCircuitOpen(sessionId, false);

    log.info(`Circuit CLOSED for session ${sessionId}`);
    this.emit('circuit:closed', sessionId);
  }

  private halfOpenCircuit(sessionId: string): void {
    const circuit = this.circuits.get(sessionId);
    if (!circuit) return;

    circuit.state = CircuitState.HALF_OPEN;
    circuit.halfOpenAt = Date.now();

    // Keep the circuit open flag so we're cautious
    // The session manager's selectSession will still avoid this session
    // until a successful submit_sm_resp comes back

    log.info(`Circuit HALF_OPEN for session ${sessionId}`);
    this.emit('circuit:half_open', sessionId);
  }

  // ─── Periodic Sweep ──────────────────────────────────────────────────────

  private startSweep(): void {
    this.sweepTimer = setInterval(() => {
      this.evaluateCircuits();
    }, 2000); // Evaluate every 2 seconds
  }

  /**
   * Evaluate all circuits and transition states as needed.
   * Called on a 2-second interval.
   */
  private evaluateCircuits(): void {
    const now = Date.now();

    for (const [sessionId, circuit] of this.circuits) {
      const errorRate = this.getErrorRate(sessionId);

      switch (circuit.state) {
        case CircuitState.CLOSED:
          // Check if error rate exceeds threshold
          if (
            errorRate >= CIRCUIT_BREAKER.ERROR_THRESHOLD &&
            circuit.requestCount >= CIRCUIT_BREAKER.MIN_REQUEST_COUNT
          ) {
            this.openCircuit(sessionId);
            this.emit('circuit:trip', sessionId, errorRate, circuit.requestCount);
          }
          break;

        case CircuitState.OPEN:
          // Check if cooldown period has elapsed
          if (
            circuit.lastOpenedAt &&
            now - circuit.lastOpenedAt >= CIRCUIT_BREAKER.COOLDOWN_MS
          ) {
            this.halfOpenCircuit(sessionId);
          }
          break;

        case CircuitState.HALF_OPEN:
          // If half-open for too long with no success, go back to open
          if (
            circuit.halfOpenAt &&
            now - circuit.halfOpenAt >= CIRCUIT_BREAKER.COOLDOWN_MS
          ) {
            // Check if any requests succeeded during half-open
            // If error rate is still high, go back to open
            if (errorRate >= CIRCUIT_BREAKER.ERROR_THRESHOLD) {
              circuit.state = CircuitState.OPEN;
              circuit.lastOpenedAt = now;
              log.warn(`Circuit back to OPEN for ${sessionId} (still high error rate)`);
            } else {
              // Enough time passed with low errors, close
              this.closeCircuit(sessionId);
            }
          }
          break;
      }
    }
  }

  // ─── Utility Methods ─────────────────────────────────────────────────────

  /**
   * Get the state of all circuit breakers.
   */
  getAllStates(): Array<{ sessionId: string; state: CircuitState; errorRate: number }> {
    const states: Array<{ sessionId: string; state: CircuitState; errorRate: number }> = [];
    for (const [sessionId, circuit] of this.circuits) {
      states.push({
        sessionId,
        state: circuit.state,
        errorRate: this.getErrorRate(sessionId),
      });
    }
    return states;
  }

  /**
   * Manually reset a circuit breaker (admin operation).
   */
  resetCircuit(sessionId: string): void {
    this.closeCircuit(sessionId);
    log.info(`Circuit ${sessionId} manually reset by admin`);
  }

  // ─── Graceful Shutdown ───────────────────────────────────────────────────

  shutdown(): void {
    if (this.sweepTimer) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = null;
    }
    this.circuits.clear();
    log.info('Circuit breaker shutdown complete');
  }
}