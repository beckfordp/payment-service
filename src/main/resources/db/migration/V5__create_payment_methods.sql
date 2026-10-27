-- US-13.1: stored payment methods. The PAN is tokenized on the way in and
-- never persisted - only the (simulated) token and display fields are.
CREATE TABLE "payment_methods" (
    id UUID PRIMARY KEY,
    customer_id TEXT NOT NULL,
    card_token TEXT NOT NULL,
    last4 TEXT NOT NULL,
    brand TEXT NOT NULL
        CONSTRAINT payment_methods_brand_check
        CHECK (brand IN ('visa', 'mastercard', 'amex', 'other')),
    expiry TEXT NOT NULL,
    billing_address JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
