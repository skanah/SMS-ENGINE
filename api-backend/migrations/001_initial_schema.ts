/**
 * 001_initial_schema.ts — Knex Database Migration
 *
 * Creates the core tables for the SMS Gateway:
 *   - users: API consumers with credit balances
 *   - campaigns: Bulk/OTP campaign tracking
 *   - messages: Individual message records with delivery status
 *   - delivery_receipts: DLR records from telcos
 *   - sender_ids: Registered Sender IDs per user
 *   - route_overrides: Admin route control overrides
 *   - dnd_registry: DND number registry
 *   - billing_transactions: Credit usage and top-up records
 *   - api_keys: API key authentication
 *   - rate_limits: Per-user rate limit tracking
 */

import { Knex } from 'knex';

export async function up(knex: Knex): Promise<void> {
  // ─── Users ────────────────────────────────────────────────────────────────
  await knex.schema.createTable('users', (table) => {
    table.uuid('id').primary().defaultTo(knex.raw('gen_random_uuid()'));
    table.string('email', 255).unique().notNullable();
    table.string('name', 255).notNullable();
    table.string('password_hash', 255).notNullable();
    table.bigInteger('credit_balance').notNullable().defaultTo(0);
    table.boolean('is_admin').notNullable().defaultTo(false);
    table.boolean('is_active').notNullable().defaultTo(true);
    table.timestamp('created_at').notNullable().defaultTo(knex.fn.now());
    table.timestamp('updated_at').notNullable().defaultTo(knex.fn.now());
  });

  // ─── API Keys ─────────────────────────────────────────────────────────────
  await knex.schema.createTable('api_keys', (table) => {
    table.uuid('id').primary().defaultTo(knex.raw('gen_random_uuid()'));
    table.uuid('user_id').notNullable().references('id').inTable('users').onDelete('CASCADE');
    table.string('key_hash', 255).unique().notNullable();
    table.string('label', 255).notNullable();
    table.boolean('is_active').notNullable().defaultTo(true);
    table.timestamp('last_used_at').nullable();
    table.timestamp('expires_at').nullable();
    table.timestamp('created_at').notNullable().defaultTo(knex.fn.now());
  });

  // ─── Sender IDs ───────────────────────────────────────────────────────────
  await knex.schema.createTable('sender_ids', (table) => {
    table.uuid('id').primary().defaultTo(knex.raw('gen_random_uuid()'));
    table.uuid('user_id').notNullable().references('id').inTable('users').onDelete('CASCADE');
    table.string('sender_id', 11).notNullable(); // Max 11 chars alphanumeric
    table.boolean('is_active').notNullable().defaultTo(true);
    table.boolean('is_locked').notNullable().defaultTo(false);
    table.integer('rotation_priority').notNullable().defaultTo(0);
    table.timestamp('created_at').notNullable().defaultTo(knex.fn.now());
    table.unique(['user_id', 'sender_id']);
  });

  // ─── Campaigns ────────────────────────────────────────────────────────────
  await knex.schema.createTable('campaigns', (table) => {
    table.uuid('id').primary().defaultTo(knex.raw('gen_random_uuid()'));
    table.uuid('user_id').notNullable().references('id').inTable('users').onDelete('CASCADE');
    table.string('name', 255).notNullable();
    table.text('message_template').notNullable();
    table.string('type', 20).notNullable().defaultTo('bulk'); // 'otp' | 'bulk' | 'transactional'
    table.string('status', 20).notNullable().defaultTo('draft'); // draft | scheduled | sending | completed | paused | cancelled
    table.integer('total_recipients').notNullable().defaultTo(0);
    table.integer('sent_count').notNullable().defaultTo(0);
    table.integer('delivered_count').notNullable().defaultTo(0);
    table.integer('failed_count').notNullable().defaultTo(0);
    table.timestamp('scheduled_at').nullable();
    table.timestamp('started_at').nullable();
    table.timestamp('completed_at').nullable();
    table.timestamp('created_at').notNullable().defaultTo(knex.fn.now());
    table.timestamp('updated_at').notNullable().defaultTo(knex.fn.now());
  });

  // ─── Messages ─────────────────────────────────────────────────────────────
  await knex.schema.createTable('messages', (table) => {
    table.uuid('id').primary().defaultTo(knex.raw('gen_random_uuid()'));
    table.uuid('user_id').notNullable().references('id').inTable('users').onDelete('CASCADE');
    table.uuid('campaign_id').nullable().references('id').inTable('campaigns').onDelete('SET NULL');
    table.string('sender', 11).notNullable();
    table.string('recipient', 20).notNullable();
    table.text('message').notNullable();
    table.string('operator', 20).nullable();
    table.string('status', 20).notNullable().defaultTo('pending'); // pending | sent | delivered | failed | expired
    table.string('encoding', 10).notNullable().defaultTo('gsm7');
    table.integer('segments').notNullable().defaultTo(1);
    table.string('smpp_message_id', 64).nullable().unique();
    table.string('session_id', 64).nullable();
    table.string('error_code', 10).nullable();
    table.timestamp('submitted_at').nullable();
    table.timestamp('delivered_at').nullable();
    table.timestamp('created_at').notNullable().defaultTo(knex.fn.now());
    table.timestamp('updated_at').notNullable().defaultTo(knex.fn.now());

    // Indexes for common queries
    table.index(['user_id', 'status']);
    table.index(['campaign_id']);
    table.index(['recipient']);
    table.index(['smpp_message_id']);
    table.index(['created_at']);
  });

  // ─── Delivery Receipts ────────────────────────────────────────────────────
  await knex.schema.createTable('delivery_receipts', (table) => {
    table.uuid('id').primary().defaultTo(knex.raw('gen_random_uuid()'));
    table.uuid('message_id').nullable().references('id').inTable('messages').onDelete('SET NULL');
    table.string('smpp_message_id', 64).notNullable();
    table.string('recipient', 20).notNullable();
    table.string('status', 20).notNullable();
    table.string('error_code', 10).nullable();
    table.string('operator', 20).nullable();
    table.string('session_id', 64).nullable();
    table.text('raw_pdu').nullable();
    table.timestamp('done_date').nullable();
    table.timestamp('submit_date').nullable();
    table.timestamp('received_at').notNullable().defaultTo(knex.fn.now());

    table.index(['smpp_message_id']);
    table.index(['status']);
    table.index(['received_at']);
  });

  // ─── Route Overrides ──────────────────────────────────────────────────────
  await knex.schema.createTable('route_overrides', (table) => {
    table.uuid('id').primary().defaultTo(knex.raw('gen_random_uuid()'));
    table.string('operator', 20).notNullable();
    table.string('action', 20).notNullable(); // 'active' | 'drain' | 'disabled'
    table.integer('priority').notNullable().defaultTo(0);
    table.uuid('overridden_by').nullable().references('id').inTable('users');
    table.timestamp('expires_at').nullable();
    table.timestamp('created_at').notNullable().defaultTo(knex.fn.now());
    table.unique(['operator']);
  });

  // ─── DND Registry ─────────────────────────────────────────────────────────
  await knex.schema.createTable('dnd_registry', (table) => {
    table.uuid('id').primary().defaultTo(knex.raw('gen_random_uuid()'));
    table.string('msisdn', 20).unique().notNullable();
    table.string('network', 20).nullable();
    table.timestamp('registered_at').notNullable().defaultTo(knex.fn.now());
    table.timestamp('expires_at').nullable();
    table.index(['msisdn']);
  });

  // ─── Billing Transactions ─────────────────────────────────────────────────
  await knex.schema.createTable('billing_transactions', (table) => {
    table.uuid('id').primary().defaultTo(knex.raw('gen_random_uuid()'));
    table.uuid('user_id').notNullable().references('id').inTable('users').onDelete('CASCADE');
    table.string('type', 20).notNullable(); // 'credit_purchase' | 'usage' | 'refund' | 'bonus'
    table.bigInteger('amount').notNullable(); // Positive for credit, negative for debit
    table.bigInteger('balance_before').notNullable();
    table.bigInteger('balance_after').notNullable();
    table.string('reference', 64).nullable().unique(); // Payment gateway reference
    table.string('description', 255).nullable();
    table.string('status', 20).notNullable().defaultTo('completed'); // pending | completed | failed
    table.timestamp('created_at').notNullable().defaultTo(knex.fn.now());

    table.index(['user_id', 'created_at']);
    table.index(['reference']);
  });

  // ─── Rate Limits ──────────────────────────────────────────────────────────
  await knex.schema.createTable('rate_limits', (table) => {
    table.uuid('id').primary().defaultTo(knex.raw('gen_random_uuid()'));
    table.uuid('user_id').notNullable().references('id').inTable('users').onDelete('CASCADE');
    table.integer('max_per_second').notNullable().defaultTo(100);
    table.integer('max_per_minute').notNullable().defaultTo(5000);
    table.integer('max_per_hour').notNullable().defaultTo(100000);
    table.timestamp('updated_at').notNullable().defaultTo(knex.fn.now());
    table.unique(['user_id']);
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists('rate_limits');
  await knex.schema.dropTableIfExists('billing_transactions');
  await knex.schema.dropTableIfExists('dnd_registry');
  await knex.schema.dropTableIfExists('route_overrides');
  await knex.schema.dropTableIfExists('delivery_receipts');
  await knex.schema.dropTableIfExists('messages');
  await knex.schema.dropTableIfExists('campaigns');
  await knex.schema.dropTableIfExists('sender_ids');
  await knex.schema.dropTableIfExists('api_keys');
  await knex.schema.dropTableIfExists('users');
}