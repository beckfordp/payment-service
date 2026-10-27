-- 'settled' -> 'captured': names the status after what it records (ADR 0011
-- amendment 2026-10-27), matching the PaymentCaptured event and the gateway's
-- capture operation.
ALTER TABLE "payment" DROP CONSTRAINT payment_status_check;
UPDATE "payment" SET status = 'captured' WHERE status = 'settled';
ALTER TABLE "payment"
    ADD CONSTRAINT payment_status_check
    CHECK (status IN ('pending', 'authorized', 'captured', 'failed'));
