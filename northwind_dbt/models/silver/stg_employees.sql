WITH source AS (
    SELECT * FROM bronze.employees
),
ajustado AS (
    SELECT
        employee_id::integer as employee_id,
        (first_name || ' ' || last_name)::varchar as full_name, -- unindo o nome completo pelo first e last name
        title::varchar as title,
        title_of_courtesy::varchar as title_of_courtesy,
        birth_date::date as birth_date,
        hire_date::date as hire_date,
        city::varchar as city,
        coalesce(region::varchar, 'N/A') as region, -- removendo nulls
        country::varchar as country,
        reports_to::integer as reports_to,
        extract(year from age(current_date, hire_date::date))::int as years_tenure
    FROM
        source
    WHERE
        employee_id is not null
)
SELECT * FROM ajustado