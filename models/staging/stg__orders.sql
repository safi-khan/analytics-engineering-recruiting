{{ config(materialized='table') }}

with source_data as (
select
   *

from {{ ref('orders') }}
)

select
   order_id
  ,provider_id
  ,order_created_at
  ,fulfillment_status
  ,line_item_count
  ,order_processed_at
  ,processing_method
  ,shop_id
  ,source_name
  ,sales_channel
  ,gross_merchandise_value_usd

from source_data
