with source_data as (
select
   *

from {{ ref('merchants') }}
)

select
  shop_id,
  merchant_created_at,
  merchant_currency,
  merchant_currency_symbol,
  is_portal_active,
  provider_id,
  merchant_updated_at,
  merchant_currency,
  benchmark_vertical,
  msm_market_segment,

from source_data
