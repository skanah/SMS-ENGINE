/**
 * paymentWebhooks.ts — Payment Gateway Webhook Handler
 *
 * Processes incoming webhooks from Nigerian payment gateways (Paystack, Flutterwave,
 * Remita, etc.) to credit user accounts when payments are confirmed.
 *
 * Key features:
 *   - Idempotent processing (prevents double-credit on webhook retries)
 *   - HMAC signature verification for each gateway
 *   - Atomic credit addition with full audit trail
 *   - Webhook event logging for debugging
 */

import { createLogger } from '../../smpp-core/logger';
import { BillingEngine } from './billingEngine';
import crypto from 'crypto';

const log = createLogger('PaymentWebhooks');

// ─── Supported Payment Gateways ──────────────────────────────────────────────

export enum PaymentGateway {
  PAYSTACK = 'paystack',
  FLUTTERWAVE = 'flutterwave',
  REMITA = 'remita',
  STRIPE = 'stripe',
  MANUAL = 'manual',
}

// ─── Webhook Payload Types ───────────────────────────────────────────────────

export interface WebhookPayload {
  gateway: PaymentGateway;
  event: string;
  reference: string;
  transactionId: string;
  amount: number;           // Amount in kobo/cents (smallest currency unit)
  currency: string;
  customerEmail?: string;
  customerId?: string;
  metadata?: Record<string, any>;
  rawBody: string;
  signature: string;
  timestamp: number;
}

export interface WebhookResult {
  processed: boolean;
  userId?: string;
  amountCredited: number;
  transactionId?: string;
  reason?: string;
}

// ─── Idempotency Store ───────────────────────────────────────────────────────

interface IdempotencyRecord {
  reference: string;
  processedAt: number;
  result: WebhookResult;
}

// ─── Payment Webhook Handler ─────────────────────────────────────────────────

export class PaymentWebhookHandler {
  private billingEngine: BillingEngine;
  private idempotencyCache: Map<string, IdempotencyRecord> = new Map();
  private readonly idempotencyTtlMs: number = 86_400_000; // 24 hours

  // Gateway secret keys (in production, loaded from env/vault)
  private secrets: Record<string, string> = {};

  constructor(billingEngine: BillingEngine) {
    this.billingEngine = billingEngine;
    this.loadSecrets();

    // Periodic cleanup of old idempotency records
    setInterval(() => this.cleanIdempotencyCache(), 60_000);
  }

  // ─── Secret Management ─────────────────────────────────────────────────────

  private loadSecrets(): void {
    this.secrets = {
      [PaymentGateway.PAYSTACK]: process.env.PAYSTACK_SECRET_KEY || 'paystack_secret_dev',
      [PaymentGateway.FLUTTERWAVE]: process.env.FLUTTERWAVE_SECRET_KEY || 'flutterwave_secret_dev',
      [PaymentGateway.REMITA]: process.env.REMITA_SECRET_KEY || 'remita_secret_dev',
      [PaymentGateway.STRIPE]: process.env.STRIPE_WEBHOOK_SECRET || 'stripe_webhook_secret_dev',
    };
  }

  /**
   * Update a gateway secret (admin operation, hot-reloadable).
   */
  setSecret(gateway: PaymentGateway, secret: string): void {
    this.secrets[gateway] = secret;
    log.info(`Updated secret for gateway: ${gateway}`);
  }

  // ─── Main Webhook Processor ────────────────────────────────────────────────

  /**
   * Process an incoming payment webhook.
   *
   * Steps:
   *   1. Verify HMAC signature
   *   2. Check idempotency (by reference)
   *   3. Map customer to internal user
   *   4. Credit the user's account
   *   5. Return result
   *
   * @param payload - Parsed webhook payload
   * @returns WebhookResult indicating success/failure
   */
  async processWebhook(payload: WebhookPayload): Promise<WebhookResult> {
    // 1. Verify signature
    if (!this.verifySignature(payload)) {
      log.warn(`Invalid signature for webhook: ${payload.reference}`);
      return {
        processed: false,
        amountCredited: 0,
        reason: 'Invalid signature',
      };
    }

    // 2. Check idempotency
    const idempotent = this.checkIdempotency(payload.reference);
    if (idempotent) {
      log.info(`Duplicate webhook detected: ${payload.reference} — returning cached result`);
      return idempotent.result;
    }

    // 3. Map customer to user
    const userId = await this.resolveUserId(payload);
    if (!userId) {
      log.warn(`No user found for webhook: ${payload.reference} (email: ${payload.customerEmail})`);
      return {
        processed: false,
        amountCredited: 0,
        reason: 'User not found',
      };
    }

    // 4. Determine credit amount (convert from kobo/cents to internal units)
    const creditAmount = this.convertToCredits(payload.amount, payload.currency);

    // 5. Credit the user
    const billingResult = await this.billingEngine.addCredits(
      userId,
      creditAmount,
      `Payment via ${payload.gateway}: ${payload.reference}`
    );

    const result: WebhookResult = {
      processed: billingResult.success,
      userId,
      amountCredited: creditAmount,
      transactionId: billingResult.transactionId,
      reason: billingResult.reason,
    };

    // 6. Cache idempotency
    this.cacheIdempotency(payload.reference, result);

    if (result.processed) {
      log.info(
        `Payment processed: ${payload.gateway} ref=${payload.reference} ` +
        `amount=${creditAmount} user=${userId}`
      );
    } else {
      log.error(
        `Payment failed: ${payload.gateway} ref=${payload.reference} ` +
        `reason=${result.reason}`
      );
    }

    return result;
  }

  // ─── Signature Verification ────────────────────────────────────────────────

  /**
   * Verify the HMAC signature of an incoming webhook.
   * Each gateway uses a different signing method.
   */
  private verifySignature(payload: WebhookPayload): boolean {
    const secret = this.secrets[payload.gateway];
    if (!secret) {
      log.warn(`No secret configured for gateway: ${payload.gateway}`);
      return false;
    }

    switch (payload.gateway) {
      case PaymentGateway.PAYSTACK:
        return this.verifyPaystackSignature(payload, secret);
      case PaymentGateway.FLUTTERWAVE:
        return this.verifyFlutterwaveSignature(payload, secret);
      case PaymentGateway.REMITA:
        return this.verifyRemitaSignature(payload, secret);
      case PaymentGateway.STRIPE:
        return this.verifyStripeSignature(payload, secret);
      case PaymentGateway.MANUAL:
        return true; // Manual credits bypass signature check
      default:
        return false;
    }
  }

  private verifyPaystackSignature(payload: WebhookPayload, secret: string): boolean {
    // Paystack: HMAC-SHA256 of raw body using secret key
    const hash = crypto
      .createHmac('sha256', secret)
      .update(payload.rawBody)
      .digest('hex');

    return crypto.timingSafeEqual(
      Buffer.from(hash),
      Buffer.from(payload.signature)
    );
  }

  private verifyFlutterwaveSignature(payload: WebhookPayload, secret: string): boolean {
    // Flutterwave: HMAC-SHA256 of raw body using secret key
    const hash = crypto
      .createHmac('sha256', secret)
      .update(payload.rawBody)
      .digest('hex');

    return hash === payload.signature;
  }

  private verifyRemitaSignature(payload: WebhookPayload, secret: string): boolean {
    // Remita: HMAC-SHA512 of reference + amount + transactionId
    const data = `${payload.reference}|${payload.amount}|${payload.transactionId}`;
    const hash = crypto
      .createHmac('sha512', secret)
      .update(data)
      .digest('hex');

    return hash === payload.signature;
  }

  private verifyStripeSignature(payload: WebhookPayload, secret: string): boolean {
    // Stripe: Uses timestamp + signature scheme
    // Simplified check — production would use stripe webhook construction
    const signedPayload = `${payload.timestamp}.${payload.rawBody}`;
    const expected = crypto
      .createHmac('sha256', secret)
      .update(signedPayload)
      .digest('hex');

    return expected === payload.signature;
  }

  // ─── User Resolution ───────────────────────────────────────────────────────

  /**
   * Resolve a webhook payload to an internal user ID.
   * Tries customer email first, then metadata user_id, then customer ID.
   */
  private async resolveUserId(payload: WebhookPayload): Promise<string | null> {
    // Check metadata for explicit user_id
    if (payload.metadata?.user_id) {
      return payload.metadata.user_id;
    }

    // Check metadata for user email
    if (payload.metadata?.email) {
      return this.findUserByEmail(payload.metadata.email);
    }

    // Use customer email from payload
    if (payload.customerEmail) {
      return this.findUserByEmail(payload.customerEmail);
    }

    // Use customer ID as fallback
    if (payload.customerId) {
      return payload.customerId;
    }

    return null;
  }

  /**
   * Find a user by email address.
   * Placeholder — in production, queries the database.
   */
  private async findUserByEmail(email: string): Promise<string | null> {
    // TODO: Query database for user by email
    // const user = await this.db('users').where('email', email).first();
    // return user?.id || null;

    // For now, return a simulated user ID
    if (email && email.includes('@')) {
      return `user-${Buffer.from(email).toString('hex').slice(0, 12)}`;
    }
    return null;
  }

  // ─── Currency Conversion ───────────────────────────────────────────────────

  /**
   * Convert payment amount (in smallest currency unit) to internal credit units.
   *
   * Examples:
   *   - ₦5,000 (500000 kobo) → 500000 credits (1:1 for NGN)
   *   - $10 (1000 cents) → 100000 credits (1 cent = 100 credits)
   */
  private convertToCredits(amount: number, currency: string): number {
    switch (currency.toUpperCase()) {
      case 'NGN':
        // NGN: 1 kobo = 1 credit
        return amount;
      case 'USD':
      case 'EUR':
      case 'GBP':
        // 1 cent = 100 credits
        return amount * 100;
      default:
        return amount;
    }
  }

  // ─── Idempotency ───────────────────────────────────────────────────────────

  private checkIdempotency(reference: string): IdempotencyRecord | null {
    const record = this.idempotencyCache.get(reference);
    if (!record) return null;

    const age = Date.now() - record.processedAt;
    if (age > this.idempotencyTtlMs) {
      this.idempotencyCache.delete(reference);
      return null;
    }

    return record;
  }

  private cacheIdempotency(reference: string, result: WebhookResult): void {
    this.idempotencyCache.set(reference, {
      reference,
      processedAt: Date.now(),
      result,
    });
  }

  private cleanIdempotencyCache(): void {
    const now = Date.now();
    for (const [key, record] of this.idempotencyCache) {
      if (now - record.processedAt > this.idempotencyTtlMs) {
        this.idempotencyCache.delete(key);
      }
    }
  }

  // ─── Gateway-Specific Parsers ──────────────────────────────────────────────

  /**
   * Parse a Paystack webhook payload into the standard format.
   */
  parsePaystackWebhook(rawBody: string, headers: Record<string, string>): WebhookPayload | null {
    try {
      const body = JSON.parse(rawBody);

      return {
        gateway: PaymentGateway.PAYSTACK,
        event: body.event,
        reference: body.data?.reference || body.data?.tx_ref || '',
        transactionId: body.data?.id?.toString() || '',
        amount: body.data?.amount || 0,
        currency: body.data?.currency || 'NGN',
        customerEmail: body.data?.customer?.email,
        metadata: body.data?.metadata,
        rawBody,
        signature: headers['x-paystack-signature'] || '',
        timestamp: Date.now(),
      };
    } catch (err: any) {
      log.error(`Failed to parse Paystack webhook: ${err.message}`);
      return null;
    }
  }

  /**
   * Parse a Flutterwave webhook payload into the standard format.
   */
  parseFlutterwaveWebhook(rawBody: string, headers: Record<string, string>): WebhookPayload | null {
    try {
      const body = JSON.parse(rawBody);

      return {
        gateway: PaymentGateway.FLUTTERWAVE,
        event: body.event,
        reference: body.data?.tx_ref || body.data?.flw_ref || '',
        transactionId: body.data?.id?.toString() || '',
        amount: body.data?.amount || 0,
        currency: body.data?.currency || 'NGN',
        customerEmail: body.data?.customer?.email,
        metadata: body.data?.meta,
        rawBody,
        signature: headers['verif-hash'] || '',
        timestamp: Date.now(),
      };
    } catch (err: any) {
      log.error(`Failed to parse Flutterwave webhook: ${err.message}`);
      return null;
    }
  }

  /**
   * Parse a Stripe webhook payload into the standard format.
   */
  parseStripeWebhook(rawBody: string, headers: Record<string, string>): WebhookPayload | null {
    try {
      const body = JSON.parse(rawBody);
      const data = body.data?.object;

      return {
        gateway: PaymentGateway.STRIPE,
        event: body.type,
        reference: data?.id || '',
        transactionId: data?.payment_intent || data?.id || '',
        amount: data?.amount_received || data?.amount || 0,
        currency: data?.currency?.toUpperCase() || 'USD',
        customerEmail: data?.receipt_email || data?.customer_email,
        metadata: data?.metadata,
        rawBody,
        signature: headers['stripe-signature'] || '',
        timestamp: body.created || Date.now(),
      };
    } catch (err: any) {
      log.error(`Failed to parse Stripe webhook: ${err.message}`);
      return null;
    }
  }
}