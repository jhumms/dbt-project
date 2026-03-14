-- This part adds in a config to say that it is for an incremental change and only if there is no schema change
{{
  config(
    materialized = 'incremental',
    on_schema_change='fail'
    )
}}

WITH src_reviews AS (
  SELECT * FROM {{ ref('src_reviews') }}
)

SELECT * FROM src_reviews
Where review_text is not NULL
{% if is_incremental() %}
    AND review_date > (SELECT max(review_date) from {{ this }})
{% endif %}