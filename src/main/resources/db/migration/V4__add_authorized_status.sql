-- ADR 0011: authorize/capture split adds 'authorized' between 'pending' and
-- 'settled' ('settled' now means captured).
ALTER TABLE "payment" DROP CONSTRAINT payment_status_check;
ALTER TABLE "payment"
    ADD CONSTRAINT payment_status_check
    CHECK (status IN ('pending', 'authorized', 'settled', 'failed'));
