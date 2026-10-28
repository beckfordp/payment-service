-- US-18.2: a fulfilment DispatchFailed voids an authorized payment's hold.
ALTER TABLE "payment" DROP CONSTRAINT payment_status_check;
ALTER TABLE "payment"
    ADD CONSTRAINT payment_status_check
    CHECK (status IN ('pending', 'authorized', 'captured', 'failed', 'voided'));
