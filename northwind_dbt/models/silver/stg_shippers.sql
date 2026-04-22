-- Removendo valores nulos
WITH source AS (
    select * from bronze.shippers
)
SELECT
    shipper_id::integer as shipper_id,
    company_name::varchar as shipper_name,
    phone::varchar as phone
FROM
    source
WHERE
    shipper_id is not null