WITH raw_feedback AS (
    SELECT * FROM {{ source ('app_log', 'application_log_data')}} 
),

aggregated_feedback AS (
    SELECT
        product_name,
        AVG(product_rating) AS avg_product_rating,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT customer_email) AS unique_customers,
        SUM(customer_sentiment_score) / COUNT(customer_email) AS avg_sentiment_score,
        COUNT(CASE WHEN sentiment = 'Positive' THEN 1 END) AS positive_feedback_count,
        COUNT(CASE WHEN sentiment = 'Negative' THEN 1 END) AS negative_feedback_count,
        COUNT(CASE WHEN sentiment = 'Neutral' THEN 1 END) AS neutral_feedback_count
    FROM raw_feedback
    GROUP BY product_name
)

SELECT
    rf.*,
    af.avg_product_rating,
    af.total_orders,
    af.unique_customers,
    af.avg_sentiment_score,
    af.positive_feedback_count,
    af.negative_feedback_count,
    af.neutral_feedback_count
FROM raw_feedback rf
LEFT JOIN aggregated_feedback af
    ON rf.product_name = af.product_name