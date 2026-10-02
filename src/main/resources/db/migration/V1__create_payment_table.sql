-- Table name is quoted throughout as a defensive habit: several plausible domain
-- names (e.g. "user", "group", "order") are reserved PostgreSQL keywords.
CREATE TABLE "payment" (
    id UUID PRIMARY KEY,
    order_id TEXT NOT NULL,
    amount_cents INT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
