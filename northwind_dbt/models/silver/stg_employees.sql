with source as (select * from bronze.employees),

cleaned as (
    select
        employee_id::integer                            as employee_id,
        first_name::varchar                             as first_name,
        last_name::varchar                              as last_name,
        (first_name || ' ' || last_name)::varchar       as full_name,
        title::varchar                                  as title,
        title_of_courtesy::varchar                      as title_of_courtesy,
        birth_date::date                                as birth_date,
        hire_date::date                                 as hire_date,
        city::varchar                                   as city,
        coalesce(region::varchar, 'N/A')                as region,
        country::varchar                                as country,
        reports_to::integer                             as reports_to,

        extract(year from age(current_date, hire_date::date))::int
                                                        as years_tenure

    from source
    where employee_id is not null
)

select * from cleaned