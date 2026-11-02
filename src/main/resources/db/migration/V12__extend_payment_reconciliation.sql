-- ADM-4/ADR 0026: the reconciliation queue's open/resolved workflow.
-- Existing first-wins flags become `open`; the recording path is unchanged.
ALTER TABLE payment_reconciliation
    ADD COLUMN status TEXT NOT NULL DEFAULT 'open',
    ADD COLUMN operator_id TEXT,
    ADD COLUMN note TEXT,
    ADD COLUMN resolved_at TIMESTAMPTZ;

ALTER TABLE payment_reconciliation
    ADD CONSTRAINT payment_reconciliation_status_check
        CHECK (status IN ('open', 'resolved'));

ALTER TABLE payment_reconciliation
    ADD CONSTRAINT payment_reconciliation_resolution_check
        CHECK (
            (status = 'open'
                AND operator_id IS NULL
                AND resolved_at IS NULL)
            OR (status = 'resolved'
                AND operator_id IS NOT NULL
                AND resolved_at IS NOT NULL)
        );

CREATE INDEX payment_reconciliation_status_created_at_idx
    ON payment_reconciliation (status, created_at DESC);
