ALTER TABLE "payment"
    ADD CONSTRAINT payment_status_check
    CHECK (status IN ('pending', 'settled', 'failed'));
