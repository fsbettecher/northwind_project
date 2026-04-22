WITH source AS (
    SELECT * FROM bronze.customers
),
ajustado AS (
    SELECT
        customer_id::varchar(10) as customer_id,
        company_name::varchar as company_name,
        contact_name::varchar as contact_name,
        contact_title::varchar as contact_title,
        address::varchar as address,
        city::varchar as city,
        coalesce(region::varchar, 'N/A') as region,
        postal_code::varchar as postal_code,
        country::varchar as country,
        phone::varchar as phone,
        fax::varchar as fax
    FROM
        source
    WHERE
        customer_id is not null
)
SELECT * FROM ajustado