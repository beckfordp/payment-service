-- TD-10.1: one payment per order. Authorize is idempotent by orderId, so a
-- retried call must find the stored row instead of placing a second hold.
ALTER TABLE "payment" ADD CONSTRAINT payment_order_id_key UNIQUE (order_id);
