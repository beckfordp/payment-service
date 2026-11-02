-- US-20.1: the expiry sweep scans only live authorizations, ordered by when
-- their hold lapses. The partial index keeps that scan off the full table.
CREATE INDEX payment_authorized_expires_at_idx
    ON "payment" (expires_at)
    WHERE status = 'authorized';
