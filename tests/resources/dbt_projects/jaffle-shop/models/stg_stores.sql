select
id,name,opened_at,tax_rate
from {{ source('ecom', 'raw_stores') }}
