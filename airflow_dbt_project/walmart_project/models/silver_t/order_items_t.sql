{{
    config(
        materialized='incremental',
        unique_key='order_item_id'
    )
}}

SELECT
    *,
    current_timestamp() AS processed_at
FROM
    {{ source('walmart_databricks', 'order_items')}}

 {% if is_incremental() %} -- if the model is in incremental mode this  code will be executed.
     WHERE updated_timestamp > (SELECT COALESCE(MAX(updated_timestamp), '1900-01-01') FROM {{ this }})
 {% endif%}
