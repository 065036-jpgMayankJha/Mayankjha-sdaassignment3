CREATE DATABASE IF NOT EXISTS ecommerce_sda;

USE ecommerce_sda;


CREATE TABLE IF NOT EXISTS clickstream (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    event_id VARCHAR(100),
    user_id VARCHAR(50),
    session_id VARCHAR(50),
    event_type VARCHAR(50),
    product_id VARCHAR(50),
    category VARCHAR(100),
    device_type VARCHAR(30),
    platform VARCHAR(30),

    event_timestamp DATETIME,

    kafka_topic VARCHAR(100),
    kafka_partition INT,
    kafka_offset BIGINT,

    consumed_at DATETIME,

    INDEX idx_clickstream_timestamp (event_timestamp),
    INDEX idx_clickstream_event_type (event_type),
    INDEX idx_clickstream_category (category)
);


CREATE TABLE IF NOT EXISTS orders (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    order_id VARCHAR(100),
    customer_id VARCHAR(100),

    product_id VARCHAR(50),
    category VARCHAR(100),

    quantity INT,
    product_price DECIMAL(10,2),

    payment_mode VARCHAR(30),
    payment_status VARCHAR(30),

    order_status VARCHAR(30),

    order_value DECIMAL(12,2),
    discount_applied DECIMAL(5,2),

    event_timestamp DATETIME,

    kafka_topic VARCHAR(100),
    kafka_partition INT,
    kafka_offset BIGINT,

    consumed_at DATETIME,

    INDEX idx_orders_timestamp (event_timestamp),
    INDEX idx_orders_status (order_status),
    INDEX idx_orders_category (category)
);


CREATE TABLE IF NOT EXISTS inventory (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    inventory_event_id VARCHAR(100),

    warehouse_id VARCHAR(50),
    product_id VARCHAR(50),
    sku VARCHAR(100),

    stock_change_type VARCHAR(50),

    quantity INT,
    current_stock_level INT,

    event_timestamp DATETIME,

    kafka_topic VARCHAR(100),
    kafka_partition INT,
    kafka_offset BIGINT,

    consumed_at DATETIME,

    INDEX idx_inventory_timestamp (event_timestamp),
    INDEX idx_inventory_warehouse (warehouse_id),
    INDEX idx_inventory_product (product_id)
);


CREATE TABLE IF NOT EXISTS logistics (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    shipment_id VARCHAR(100),
    order_id VARCHAR(100),

    courier_partner VARCHAR(100),

    current_status VARCHAR(50),
    current_location VARCHAR(100),

    expected_delivery_date DATE,

    event_timestamp DATETIME,

    kafka_topic VARCHAR(100),
    kafka_partition INT,
    kafka_offset BIGINT,

    consumed_at DATETIME,

    INDEX idx_logistics_timestamp (event_timestamp),
    INDEX idx_logistics_status (current_status),
    INDEX idx_logistics_courier (courier_partner)
);