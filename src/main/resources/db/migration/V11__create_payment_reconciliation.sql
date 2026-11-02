-- US-21.4: a void/capture loser or other contradictory terminal outcome.
-- First-wins: the first reason recorded is the one an operator sees.
create table payment_reconciliation (
    order_id text primary key
        references "payment" (order_id) on delete cascade,
    reason text not null,
    created_at timestamptz not null default now()
);
