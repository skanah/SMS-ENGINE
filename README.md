<div align="center">

# 🚀 SMS-ENGINE

### Production-Grade SMS Gateway Engine for Nigerian Telecommunications

**MTN • Airtel • Glo • 9mobile — SMPP 3.4 | High-Throughput | DND-Compliant**

[![Node.js](https://img.shields.io/badge/Node.js-≥18-339933?style=flat-square&logo=node.js&logoColor=white)](https://nodejs.org)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.5-3178C6?style=flat-square&logo=typescript&logoColor=white)](https://www.typescriptlang.org)
[![Fastify](https://img.shields.io/badge/Fastify-5.x-000000?style=flat-square&logo=fastify&logoColor=white)](https://fastify.io)
[![SMPP](https://img.shields.io/badge/SMPP-3.4-FF6C37?style=flat-square)](https://en.wikipedia.org/wiki/Short_Message_Peer-to-Peer)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-Streaming-231F20?style=flat-square&logo=apache-kafka&logoColor=white)](https://kafka.apache.org)
[![Redis](https://img.shields.io/badge/Redis-Cache-DC382D?style=flat-square&logo=redis&logoColor=white)](https://redis.io)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14+-4169E1?style=flat-square&logo=postgresql&logoColor=white)](https://postgresql.org)

</div>

---

## 📖 Table of Contents

- [Overview](#-overview)
- [Key Features](#-key-features)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Prerequisites](#-prerequisites)
- [Installation](#-installation)
- [Configuration](#-configuration)
- [Running the Application](#-running-the-application)
- [API Reference](#-api-reference)
- [Database Schema](#-database-schema)
- [Core Components Deep Dive](#-core-components-deep-dive)
- [Dashboard](#-dashboard)
- [Performance & Scaling](#-performance--scaling)
- [Security](#-security)
- [Roadmap](#-roadmap)
- [Contributing](#-contributing)
- [License](#-license)

---

## 🎯 Overview

**SMS-ENGINE** is a high-throughput, production-grade SMS gateway engineered for the Nigerian telecommunications landscape. It aggregates direct **SMPP 3.4** bindings to all four major Nigerian mobile network operators — **MTN, Airtel, Glo, and 9mobile** — behind a single, resilient REST API, and augments them with **HTTP aggregator routes** (Termii, BulkSMS Nigeria) for cost-optimized and rapid deployment scenarios.

The engine is designed to sustain tens of thousands of messages per second through credit-based sliding-window SMPP pipelining, Kafka-backed async ingestion, per-session circuit breakers, and full **NCC DND (Do-Not-Disturb) regulatory compliance** — all with real-time billing, delivery-receipt (DLR) processing, and an operator-grade monitoring dashboard.

> **Design target:** ~50,000 MPS with ~2,000 MPS per SMPP link, requiring 25–30 concurrent TRX bindings across the four telcos.

---

## ✨ Key Features

### 📡 SMPP Core
- **Multi-telco transceiver pool** — up to 30 concurrent SMPP TRX sessions, weighted by market share (MTN ×3, Airtel ×2, Glo ×2, 9mobile ×1)
- **128-credit sliding window** — aggressive `submit_sm` pipelining without awaiting individual `submit_sm_resp` PDUs, with backpressure buffering (100k queue)
- **Per-session circuit breaker** — rolling 30s error-rate monitoring (15% trip threshold) with CLOSED → OPEN → HALF_OPEN state machine and automatic drain of failing links
- **Automatic reconnection** — exponential backoff with configurable jitter (`RECONNECT_DELAY_MS` → `RECONNECT_MAX_DELAY_MS`)
- **Enquire-link heartbeats** — 30s keepalive with session health scoring (response times, error rates, uptime)
- **DLR handler** — high-volume delivery-receipt buffering, parsing, and persistence
- **GSM 7-bit / UCS-2 encoding** — spec-correct 3GPP TS 23.038 bit-packing, UDH concatenation for multipart SMS, and auto encoding selection

### 🛣️ Intelligent Routing
- **Trie-based prefix matcher** — sub-microsecond lookup of every known Nigerian MSISDN prefix to its operator
- **DND compliance engine** — Nigerian Do-Not-Disturb regulation enforcement: OTP/transactional traffic routes via whitelisted corporate binds, promotional traffic to DND numbers is rejected or diverted
- **Sender ID rotator** — automatic alphanumeric sender ID rotation when delivery success rate drops below threshold
- **Intent-based aggregator routing** — transactional (OTP) → Termii fast lane, promotional (bulk) → BulkSMS Nigeria cost lane, with rolling-window provider health tracking and automatic failover
- **Admin route overrides** — drain, disable, or re-prioritize any telco route at runtime (with optional TTL expiry)

### ⚙️ API & Backend
- **Fastify REST API** — <2ms ingestion pipeline: Zod validation → prefix lookup → DND check → Kafka publish → immediate 202 response
- **Kafka priority lanes** — separate `sms-high-priority-otp` and `sms-low-priority-bulk` topics with consumer-group processing (3 concurrent workers, 6 partitions)
- **Campaign stream processor** — async batch processing bridge between REST ingestion and the SMPP core
- **Billing engine** — per-segment credit billing (₦1.00 GSM7 / ₦1.50 UCS-2 + ₦0.50 overhead), optimistic-locking credit reservation to prevent double-spend, full audit trail
- **Payment gateway webhooks** — Paystack, Flutterwave, Stripe, Remita with HMAC signature verification and idempotent credit processing
- **PostgreSQL persistence** — Knex migrations for users, campaigns, messages, DLRs, sender IDs, route overrides, DND registry, billing, API keys, and rate limits

### 📊 Observability & Ops
- **Pino structured logging** with per-component namespaced loggers
- **Periodic telemetry stats** (every 10s): throughput, queue depth, window credits, timeouts, DLR buffer
- **Admin telemetry API** — session states, circuit states, per-operator error rates
- **Zero-dependency test dashboard** — standalone HTML dashboard with live operator/session/circuit monitoring
- **Graceful shutdown** — reverse-order component teardown on SIGTERM/SIGINT
- **Redis single-node & cluster auto-detection** via connection string

---

## 🏗️ Architecture

```
                            ┌─────────────────────────────────────────────────┐
                            │                  CLIENTS                        │
                            │        (Web App • REST Consumers • Webhooks)     │
                            └───────────────────────┬─────────────────────────┘
                                                    │ HTTPS / JWT
                            ┌───────────────────────▼─────────────────────────┐
                            │              FASTIFY REST API (:8081)            │
                            │  POST /sms/send · /admin/* · /webhooks/*         │
                            │  Zod validation → prefix lookup → DND check       │
                            └──────┬──────────────────────────────┬───────────┘
                                   │                      (fast path < 2ms)
                    ┌──────────────▼──────────┐        ┌──────────▼──────────┐
                    │   APACHE KAFKA LANES     │        │   AGGREGATOR ROUTER  │
                    │  • high-priority-otp     │        │  Termii (OTP lane)   │
                    │  • low-priority-bulk     │        │  BulkSMS (promo lane)│
                    └──────────────┬───────────┘        │  + health failover   │
                                   │                    └──────────────────────┘
                    ┌──────────────▼───────────────────┐
                    │   CAMPAIGN STREAM PROCESSOR       │
                    │   (consumer group, 3 workers)     │
                    │   credit reservation → billing    │
                    └──────────────┬───────────────────┘
                                   │
        ┌──────────────────────────▼───────────────────────────────────┐
        │                    SMPP CORE ENGINE                           │
        │  ┌────────────────┐ ┌──────────────┐ ┌────────────────────┐  │
        │  │ Sliding Window │ │   Circuit    │ │   DLR Handler      │  │
        │  │  (128 credits) │ │   Breaker    │ │ (receipt buffer)    │  │
        │  └───────┬────────┘ └──────┬───────┘ └─────────┬──────────┘  │
        │  ┌───────▼──────────────────▼──────────────────▼──────────┐  │
        │  │            SMPP SESSION MANAGER (TRX pool)              │  │
        │  └─────────────────────────────────────────────────────────┘  │
        └─────┬─────────────┬─────────────┬──────────────┬───────────────┘
              │             │             │              │
        ┌─────▼────┐ ┌──────▼─────┐ ┌─────▼────┐ ┌───────▼───────┐
        │   MTN    │ │   AIRTEL   │ │   GLO    │ │    9MOBILE    │
        │ smpp:2775│ │ smpp:2775  │ │ smpp:2775│ │   smpp:2775   │
        └──────────┘ └────────────┘ └──────────┘ └───────────────┘

        ┌───────────────────┐  ┌──────────────┐  ┌────────────────────┐
        │  REDIS            │  │  POSTGRESQL  │  │  PAYMENT GATEWAYS  │
        │ DND set • health  │  │ Knex schema  │  │ Paystack • Flutter-│
        │ overrides • cache │  │ + audit trail│  │ wave • Stripe      │
        └───────────────────┘  └──────────────┘  └────────────────────┘
```

---

## 🧰 Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Language** | TypeScript 5.5 (strict) | Type safety across the entire engine |
| **HTTP API** | Fastify 5 + @fastify/jwt, cors, multipart | High-performance REST ingestion |
| **SMPP** | smpp (SMPP 3.4) | Direct telco transceiver bindings |
| **Message streaming** | KafkaJS | OTP/bulk priority lanes, DLR streaming |
| **Caching / registry** | ioredis (single-node & cluster) | DND set, route overrides, health state |
| **Database** | PostgreSQL + Knex | Users, campaigns, messages, DLRs, billing |
| **Validation** | Zod | Sub-millisecond request validation |
| **Logging** | Pino | Structured, namespaced production logging |
| **Billing** | Custom engine | Credit reservation, audit trail |
| **Payments** | Paystack, Flutterwave, Stripe, Remita | Wallet top-ups via verified webhooks |
| **Aggregators** | Termii, BulkSMS Nigeria | HTTP fallback lanes (OTP vs promotional) |
| **Auth** | JWT + API keys | User & machine authentication |

---

## 📁 Project Structure

```
smsP/
├── smpp-core/                    # Core SMPP engine
│   ├── index.ts                  # Bootstrap: init order + graceful shutdown
│   ├── config.ts                 # All tunables: telco binds, windows, thresholds
│   ├── types.ts                  # Shared interfaces & enums
│   ├── smppSessionManager.ts     # TRX session pool per telco
│   ├── slidingWindowProcessor.ts # 128-credit submit_sm pipelining
│   ├── circuitBreaker.ts         # Rolling-window per-session breaker
│   ├── dlrHandler.ts             # Delivery receipt buffering/persistence
│   ├── dndRouter.ts              # NCC DND compliance engine
│   ├── senderIdRotator.ts        # Auto sender ID rotation
│   ├── prefixMatcher.ts          # Trie-based operator prefix lookup
│   ├── gsmProcessor.ts           # GSM7/UCS-2 segmentation + UDH
│   ├── logger.ts                 # Pino logger factory
│   ├── lib/
│   │   ├── gsm7BitPacking.ts     # 3GPP TS 23.038 bit-packing
│   │   └── redisClient.ts        # Redis/cluster factory + helpers
│   └── aggregator/               # HTTP aggregator lane
│       ├── aggregatorRouter.ts    # Intent-based routing + health failover
│       ├── aggregatorDispatcher.ts# Dispatch with retries/timeouts
│       ├── termiiClient.ts        # Termii API client (OTP lane)
│       ├── bulksmsClient.ts       # BulkSMS Nigeria client (promo lane)
│       └── aggregatorTypes.ts     # Client/health contracts
├── api-backend/                  # REST API layer
│   ├── apiServer.ts              # Fastify bootstrap (wired into smpp-core)
│   ├── standaloneServer.ts       # Mock API for frontend testing
│   ├── routes/
│   │   ├── sendSms.ts             # POST /api/v1/sms/send
│   │   ├── admin.ts               # Admin: overrides, DND, telemetry
│   │   └── webhooks.ts            # Payment + aggregator DLR webhooks
│   ├── services/
│   │   ├── billingEngine.ts       # Credit billing & reservation
│   │   ├── campaignStreamProcessor.ts # Kafka consumer → SMPP bridge
│   │   └── paymentWebhooks.ts     # Gateway webhook verification/credit
│   └── migrations/
│       └── 001_initial_schema.ts  # Full PostgreSQL schema (Knex)
├── frontend/
│   ├── standalone.html           # Zero-dependency live dashboard
│   └── components/               # React dashboard components
│       ├── DashboardOverview.tsx
│       ├── SendSmsForm.tsx
│       ├── CampaignsList.tsx
│       └── admin/RouteControl.tsx
├── @types/                       # Global type augmentations
├── package.json
├── tsconfig.json
├── .env.example                  # Configuration template
└── README.md
```

---

## ✅ Prerequisites

| Dependency | Version | Notes |
|---|---|---|
| **Node.js** | ≥ 18.x | 20 LTS recommended |
| **npm** | ≥ 9.x | Ships with Node |
| **PostgreSQL** | ≥ 14 | Needs `pgcrypto` extension for `gen_random_uuid()` |
| **Redis** | ≥ 6 | Single node or cluster (auto-detected) |
| **Apache Kafka** | ≥ 2.8 | ZooKeeper or KRaft mode |
| **SMPP bindings** | — | Credentials with each telco (or use aggregator lanes instead) |

---

## 🔧 Installation

```bash
# 1. Clone the repository
git clone https://github.com/skanah/SMS-ENGINE.git
cd SMS-ENGINE

# 2. Install dependencies
npm install

# 3. Configure environment
cp .env.example .env
#    → Edit .env with your telco/aggregator credentials and secrets

# 4. Create the database
createdb sms_gateway
psql -d sms_gateway -c "CREATE EXTENSION IF NOT EXISTS pgcrypto;"

# 5. Run migrations
npx knex migrate:latest \
  --knexfile api-backend/migrations/knexfile.ts   # or run 001_initial_schema.ts manually

# 6. Compile TypeScript
npm run build
```

---

## ⚙️ Configuration

All configuration is loaded from `.env` at startup (see [`.env.example`](./.env.example)). Key groups:

<details>
<summary><b>Telco SMPP bindings</b> (per operator)</summary>

```env
MTN_HOST=smpp.mtn.net.ng          MTN_PORT=2775          MTN_SYSTEM_ID=...  MTN_PASSWORD=...
AIRTEL_HOST=smpp.airtel.com.ng    AIRTEL_PORT=2775       AIRTEL_SYSTEM_ID=...
GLO_HOST=smpp.glo.com.ng          GLO_PORT=2775          GLO_SYSTEM_ID=...
NINEMOBILE_HOST=smpp.9mobile.com.ng  NINEMOBILE_PORT=2775  NINEMOBILE_SYSTEM_ID=...
```
</details>

<details>
<summary><b>Infrastructure</b> (Kafka / Redis / PostgreSQL / API)</summary>

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
REDIS_URL=localhost:6379            # redis:// | rediss:// | comma list = cluster
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/sms_gateway
API_PORT=8081
JWT_SECRET=...                      # min 32 chars
JWT_EXPIRY=24h
```
</details>

<details>
<summary><b>Aggregator lanes</b> (Termii / BulkSMS Nigeria)</summary>

```env
TERMII_API_KEY=...                  # https://developers.termii.com/
TERMII_SENDER_ID=SMSAlert
TERMII_COST_PER_SEGMENT=65          # kobo
BULKSMS_USERNAME=...                # https://www.bulksmsnigeria.com/
BULKSMS_SENDER_ID=SMSNotify
BULKSMS_COST_PER_SEGMENT=40         # kobo

ROUTE_TRANSACTIONAL_PROVIDER=termii
ROUTE_PROMOTIONAL_PROVIDER=bulksms
ROUTE_FALLBACK_PROVIDER=termii
```
</details>

<details>
<summary><b>Engine tunables</b></summary>

```env
SLIDING_WINDOW_SIZE=128             # submit_sm credits per session
MAX_CONCURRENT_SESSIONS=30          # total TRX bindings across telcos
HEARTBEAT_INTERVAL_MS=30000         # enquire_link period
RECONNECT_DELAY_MS=5000             # backoff base
RECONNECT_MAX_DELAY_MS=30000        # backoff ceiling
```
</details>

<details>
<summary><b>Payment gateways</b></summary>

```env
PAYSTACK_SECRET_KEY=sk_...
FLUTTERWAVE_SECRET_KEY=FLWSECK-...
MONNIFY_API_KEY=...  MONNIFY_SECRET_KEY=...  MONNIFY_CONTRACT_CODE=...
```
</details>

---

## ▶️ Running the Application

```bash
# Development (ts-node, full SMPP + Kafka + Redis stack)
npm run dev

# Production (compiled output)
npm run build
npm start

# Lint
npm run lint

# Lightweight mock API (no Redis/Kafka/SMPP — frontend testing only)
npx ts-node api-backend/standaloneServer.ts

# Live monitoring dashboard (served from the mock API)
# → open frontend/standalone.html in your browser
```

### Startup sequence

1. Load environment configuration (`dotenv`)
2. Connect Redis (single-node or cluster auto-detected)
3. Initialize Kafka client (producer + DLR topics)
4. Build SMPP session pool (weighted per-telco TRX bindings)
5. Start sliding-window processor (128 credits/session, 100k queue)
6. Arm per-session circuit breakers
7. Start DLR handler, DND router, sender ID rotator
8. Register aggregator clients (Termii, BulkSMS)
9. Start Fastify API on `API_PORT` (default `:8081`)
10. Log READY banner + periodic stats every 10s

`SIGTERM`/`SIGINT` trigger a graceful, reverse-order shutdown (drain API → rotator → DLR → breaker → window → sessions → Redis).

---

## 🌐 API Reference

### Send SMS

```http
POST /api/v1/sms/send
Authorization: Bearer <JWT>
Content-Type: application/json

{
  "sender": "MyBrand",           // 1–11 alphanumeric chars
  "recipient": "08031234567",     // NG number, auto-normalized to E.164
  "message": "Your OTP is 482913",
  "priority": "otp",              // 'otp' | 'transactional' | 'bulk' (default)
  "encoding": "auto",            // 'gsm7' | 'ucs2' | 'auto'
  "campaignId": "camp_abc123",   // optional
  "scheduleAt": 1730000000000    // optional unix ms
}
```

**Response — `202 Accepted`:**

```json
{
  "success": true,
  "messageId": "MSG1730000000AB12",
  "cost": 150,
  "operator": "mtn",
  "dndAction": "normal"
}
```

| Field | Description |
|---|---|
| `messageId` | Gateway tracking ID for DLR correlation |
| `cost` | Estimated cost in kobo (segments × rate + overhead) |
| `operator` | Detected telco via prefix Trie |
| `dndAction` | `normal` \| `route_corporate` \| `route_marketing` \| `reject` |

**Pipeline (target < 2ms):** Zod validation → E.164 normalization → Trie prefix lookup → Redis DND check → Kafka publish (OTP vs bulk lane) → `202` response.

### Health

```http
GET /api/v1/health          # public service heartbeat
GET /api/v1/admin/health    # authenticated: sessions, circuits, throughput
```

### Admin (JWT + admin role)

```http
GET   /api/v1/admin/routes                  # per-operator session & circuit state
POST  /api/v1/admin/routes/override         # { operator, action: active|drain|disabled, ... }
GET   /api/v1/admin/telemetry               # full system telemetry snapshot
POST  /api/v1/admin/dnd/add                 # add MSISDN to DND registry
POST  /api/v1/admin/dnd/remove              # remove MSISDN from DND registry
GET   /api/v1/admin/aggregators/health      # Termii/BulkSMS success-rate snapshot
POST  /api/v1/admin/aggregators/override    # force intent → provider routing
```

### Webhooks (signature-verified, unauthenticated by design)

```http
POST /api/v1/webhooks/paystack       # Paystack charge.success → credit wallet
POST /api/v1/webhooks/flutterwave    # Flutterwave payment → credit wallet
POST /api/v1/webhooks/stripe         # Stripe events
POST /api/v1/webhooks/termii/dlr     # Termii delivery receipt callback
POST /api/v1/webhooks/bulksms/dlr    # BulkSMS Nigeria delivery receipt
```

---

## 🗄️ Database Schema

Ten PostgreSQL tables created by Knex migration `001_initial_schema.ts`:

| Table | Purpose |
|---|---|
| `users` | API consumers with credit balances (kobo) and roles |
| `api_keys` | Hashed API keys per user with expiry tracking |
| `sender_ids` | Registered alphanumeric sender IDs with rotation priority |
| `campaigns` | Bulk/OTP campaigns with live progress counters |
| `messages` | Per-message records with routing + delivery status |
| `delivery_receipts` | Raw DLR records from telcos/aggregators |
| `route_overrides` | Admin route actions (active/drain/disabled) with TTL |
| `dnd_registry` | DND MSISDN registry with network tagging |
| `billing_transactions` | Append-only credit ledger (purchase/usage/refund) |
| `rate_limits` | Per-user throughput caps (per second/minute/hour) |

---

## 🔍 Core Components Deep Dive

### Sliding Window Processor
Credit-based `submit_sm` pipelining — the engine fires PDUs up to the 128-credit window without waiting for responses, decrements credits on each `submit_sm_resp`, queues when the window is full (backpressure), and sweeps for lost-in-window packets. Batches up to 200 messages per tick and picks the healthiest session per operator.

### Circuit Breaker
Per-session rolling 30s error-rate monitor. Exceeding the 15% threshold trips the circuit to OPEN (traffic diverted), and after a cooldown a HALF_OPEN probe tests recovery before restoring the session. Failure of a single MTN link drains only that link — the remaining pool keeps operating.

### DND Router
Nigerian regulatory compliance: DND numbers (Redis `SISMEMBER`, sub-millisecond, with local-cache fallback and 60s refresh) route OTP/transactional traffic through whitelisted corporate binds, while promotional traffic to DND numbers is rejected or diverted to a lower-priority marketing lane.

### Prefix Matcher
A frozen Trie built at startup from the complete Nigerian prefix table maps any MSISDN to its operator in O(L) — sub-microsecond lookups, shared across worker threads.

### Aggregator Router
Intent-based dual-lane routing: **transactional → Termii** (fast, ~₦0.65/segment), **promotional → BulkSMS Nigeria** (cheap, ~₦0.40/segment). Rolling 5-minute health windows with a 10% error threshold trigger automatic failover; admins can pin providers via Redis-backed overrides (24h TTL).

### Billing Engine
Optimistic-locking credit reservation prevents double-spend under concurrency. Costs: ₦1.00 per GSM-7 segment, ₦1.50 per UCS-2 segment, + ₦0.50 per-message overhead — with an append-only transaction audit trail and low-balance alerts.

---

## 📊 Dashboard

`frontend/standalone.html` is a zero-dependency live dashboard:

- **Send SMS tab** — test submissions with segment/cost preview
- **Dashboard tab** — 5-second auto-refresh of:
  - Throughput metrics (submitted / delivered / failed / delivery rate / uptime)
  - Per-operator panels: session states, circuit states, avg response times, window usage

The `frontend/components/` directory contains the React equivalents (`DashboardOverview`, `SendSmsForm`, `CampaignsList`, admin `RouteControl`) for integration into a full React app.

---

## ⚡ Performance & Scaling

| Design decision | Payoff |
|---|---|
| 128-credit sliding window per session | ~2,000 MPS per SMPP link without head-of-line blocking |
| 30 weighted TRX bindings | ~50,000 MPS aggregate across 4 telcos |
| Trie prefix matching | Sub-microsecond operator resolution |
| Kafka priority lanes | OTP traffic never queues behind bulk |
| Redis DND set + local cache | Sub-millisecond compliance checks |
| 100k in-memory queue | Backpressure absorption under burst load |
| Designed for `cluster`/`worker_threads` | Each worker bootstraps its own engine instance |

---

## 🔒 Security

- **JWT authentication** on all user/admin routes with `@fastify/jwt`
- **API key hashing** — keys are never stored in plaintext
- **HMAC webhook verification** for every payment gateway (Paystack, Flutterwave, Stripe)
- **Idempotent webhook processing** — replayed events never double-credit wallets
- **Zod strict validation** on every request body
- **Per-user rate limits** enforced at second/minute/hour granularity
- **Secrets via environment** — no credentials in code; `.env` is git-ignored

---

## 🗺️ Roadmap

- [ ] Webhook DLR fan-out to customer callbacks
- [ ] Web UI: campaigns composer with CSV contact upload
- [ ] Kubernetes Helm chart + horizontal SMPP worker scaling
- [ ] Prometheus metrics endpoint
- [ ] International route fallback pool
- [ ] 2FA / TOTP for admin accounts (`otplib` + `qrcode` already in dependencies)

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

Please ensure `npm run lint` passes and new code is strictly typed.

---

## 📄 License

This project is licensed under the MIT License — see the repository for details.

---

<div align="center">

**Built for the Nigerian telecom ecosystem** 🇳🇬

[Report an issue](https://github.com/skanah/SMS-ENGINE/issues) · [View the dashboard](./frontend/standalone.html) · [Configuration reference](./.env.example)

</div>

