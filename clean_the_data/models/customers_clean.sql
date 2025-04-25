{{
    config(
        materialized='table',
        database='raw_data.db',
        schema='main'
    )
}}

-- Clean customers data by removing null credit scores
SELECT *
FROM customers_raw
WHERE CreditScore IS NOT NULL 