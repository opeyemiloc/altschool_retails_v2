-- ==============================================================================
-- RETAIL ANALYTICS PLATFORM - DDL INITIALIZATION SCRIPT
-- ==============================================================================
-- This script creates the schema and empty tables for the operational database.
-- Data loading is handled by the Python application pipeline after validation.
-- ==============================================================================

-- Create schema
CREATE SCHEMA IF NOT EXISTS ECOMMERCE;

-- Create customers table
CREATE TABLE IF NOT EXISTS ECOMMERCE.CUSTOMERS
(
    customer_id UUID PRIMARY KEY,
    customer_unique_id UUID NOT NULL,
    customer_zip_code_prefix VARCHAR(20) NOT NULL,
    customer_city VARCHAR(255) NOT NULL,
    customer_state VARCHAR(2) NOT NULL
);

-- Create geolocation table
CREATE TABLE IF NOT EXISTS ECOMMERCE.GEOLOCATION
(
    geolocation_zip_code_prefix NUMERIC(20) NOT NULL,
    geolocation_latitude NUMERIC(20, 6) NOT NULL,
    geolocation_longitude NUMERIC(20, 6) NOT NULL,
    geolocation_city VARCHAR(255) NOT NULL,
    geolocation_state VARCHAR(2) NOT NULL
);

-- Create orders table
CREATE TABLE IF NOT EXISTS ECOMMERCE.ORDERS
(
    order_id UUID PRIMARY KEY,
    customer_id UUID NOT NULL,
    order_status VARCHAR(50) NOT NULL,
    order_purchase_timestamp TIMESTAMP NOT NULL,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

-- Create order items table
CREATE TABLE IF NOT EXISTS ECOMMERCE.ORDER_ITEMS
(
    order_id UUID NOT NULL,
    order_item_id SERIAL,
    product_id UUID NOT NULL,
    seller_id UUID NOT NULL,
    shipping_limit_date TIMESTAMP NOT NULL,
    price NUMERIC(10, 2) NOT NULL,
    freight_value NUMERIC(10, 2) NOT NULL
);

-- Create order payments table
CREATE TABLE IF NOT EXISTS ECOMMERCE.ORDER_PAYMENTS
(
    order_id UUID NOT NULL,
    payment_sequential INT NOT NULL,
    payment_type VARCHAR(20) NOT NULL,
    payment_installments INT NOT NULL,
    payment_value NUMERIC(10, 2) NOT NULL
);

-- Create order reviews table
CREATE TABLE IF NOT EXISTS ECOMMERCE.ORDER_REVIEWS
(
    review_id UUID NOT NULL,
    order_id UUID NOT NULL,
    review_score INT NOT NULL,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP NOT NULL,
    review_answer_timestamp TIMESTAMP NOT NULL
);

-- Create products table
CREATE TABLE IF NOT EXISTS ECOMMERCE.PRODUCTS
(
    product_id UUID PRIMARY KEY,
    product_category_name VARCHAR(255),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);

-- Create sellers table
CREATE TABLE IF NOT EXISTS ECOMMERCE.SELLERS
(
    seller_id UUID PRIMARY KEY,
    seller_zip_code_prefix VARCHAR(20) NOT NULL,
    seller_city VARCHAR(255) NOT NULL,
    seller_state VARCHAR(2) NOT NULL
);

-- Create product category name translation table
CREATE TABLE IF NOT EXISTS ECOMMERCE.PRODUCT_CATEGORY_NAME_TRANSLATION
(
    product_category_name VARCHAR(255) PRIMARY KEY,
    product_category_name_english VARCHAR(255) NOT NULL
);
