with source as (select * from bronze.shippers)

select
    shipper_id::integer     as shipper_id,
    company_name::varchar   as shipper_name,
    phone::varchar          as phone
from source
where shipper_id is not null