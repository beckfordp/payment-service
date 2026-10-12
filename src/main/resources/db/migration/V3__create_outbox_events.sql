-- Transactional outbox (ADR 0008 / TD-3.4): payment-service writes its
-- outbound Kafka events here, inside the same transaction as the state
-- change, instead of calling a Kafka producer directly. A later Debezium
-- connector (TD-3.1, gluon-root infra) tails this table's WAL and streams
-- rows to "payment-events" — not built yet, this table is self-contained.
CREATE TABLE "outbox_events" (
    id UUID PRIMARY KEY,
    aggregate_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    trace_parent TEXT,
    correlation_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
