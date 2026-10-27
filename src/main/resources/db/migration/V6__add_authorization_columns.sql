-- US-14.1: POST /payments/authorize records its outcome on the payment row.
-- All nullable - rows from the generated CRUD routes never set them.
-- expires_at is what Checkout Journey 5's expiry sweep will poll against.
ALTER TABLE "payment"
    ADD COLUMN payment_method_id UUID REFERENCES "payment_methods" (id),
    ADD COLUMN auth_id TEXT,
    ADD COLUMN expires_at TIMESTAMPTZ,
    ADD COLUMN decline_reason TEXT;
