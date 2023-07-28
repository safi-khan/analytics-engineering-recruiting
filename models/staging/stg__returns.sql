with source_data as (
select
   *

from {{ ref('orders') }}
)

select
   return_id
  ,return_Created_at
  ,destination_id
  ,item_count
  ,is_gift_return
  ,is_in_store_return
  ,order_id
  ,provider_id
  ,return_policy
  ,return_processed_at
  ,return_state
  ,shop_id
  ,refund_value_usd
  ,exchange_value_usd
  ,return_price_usd
  ,total_shop_currency

from source_data
