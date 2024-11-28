{{ config (
    materialized = 'table'
)}}

SELECT
    customer_name,
    customer_email,
    customer_gender,
    customer_age_group,
    customer_loyalty_program,
    product_category,
    product_sub_category,
    product_name,
    product_rating,
    product_review_text,
    product_return_status,
    product_issue_type,
    order_id,
    order_status,
    purchase_mode,
    payment_mode,
    discount_applied,
    store_location,
    delivery_status,
    order_fulfillment_time,
    response_time,
    follow_up_required,
    feedback_date,
    feedback_category,
    feedback_sub_category,
    sentiment,
    customer_sentiment_score,
    feedback_text_length,
    customer_support_rating,
    resolution_status,
    avg_product_rating,
    total_orders,
    unique_customers,
    avg_sentiment_score,
    positive_feedback_count,
    negative_feedback_count,
    neutral_feedback_count
FROM
    {{ ref('stage_src_customer_feedback') }} 