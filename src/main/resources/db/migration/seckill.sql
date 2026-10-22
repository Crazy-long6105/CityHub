-- Apply once, with old order producers/consumers stopped. Do not reload hmdp.sql.
-- Audit existing active orders and populate qualifications before enabling traffic.
CREATE TABLE tb_voucher_purchase_qualification (
    user_id BIGINT UNSIGNED NOT NULL,
    voucher_id BIGINT UNSIGNED NOT NULL,
    order_id BIGINT NOT NULL,
    create_time DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (user_id, voucher_id),
    UNIQUE KEY uk_purchase_order (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
