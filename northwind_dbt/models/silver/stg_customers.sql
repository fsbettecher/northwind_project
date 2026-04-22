with source as (select * from bronze.customers),

cleaned as (
    select
        customer_id::varchar(10)                    as customer_id,
        company_name::varchar                       as company_name,
        contact_name::varchar                       as contact_name,
        contact_title::varchar                      as contact_title,
        address::varchar                            as address,
        city::varchar                               as city,
        coalesce(region::varchar, 'N/A')            as region,
        postal_code::varchar                        as postal_code,
        country::varchar                            as country,
        phone::varchar                              as phone,
        fax::varchar                                as fax,

        case country
            when 'USA'         then 'América do Norte'
            when 'Canada'      then 'América do Norte'
            when 'Mexico'      then 'América Latina'
            when 'Brazil'      then 'América Latina'
            when 'Venezuela'   then 'América Latina'
            when 'Argentina'   then 'América Latina'
            when 'UK'          then 'Europa'
            when 'Germany'     then 'Europa'
            when 'France'      then 'Europa'
            when 'Spain'       then 'Europa'
            when 'Italy'       then 'Europa'
            when 'Sweden'      then 'Europa'
            when 'Denmark'     then 'Europa'
            when 'Norway'      then 'Europa'
            when 'Finland'     then 'Europa'
            when 'Belgium'     then 'Europa'
            when 'Switzerland' then 'Europa'
            when 'Austria'     then 'Europa'
            when 'Portugal'    then 'Europa'
            when 'Poland'      then 'Europa'
            when 'Ireland'     then 'Europa'
            else 'Outros'
        end                                         as continent

    from source
    where customer_id is not null
)

select * from cleaned