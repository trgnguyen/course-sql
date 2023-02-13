with customers as (
    select 
        customer_address.customer_id
        , customer_data.first_name || ' ' || customer_data.last_name as customer_name
        , customer_address.customer_city
        , customer_address.customer_state
        , us_cities.geo_location
    from vk_data.customers.customer_address
    inner join vk_data.customers.customer_data using(customer_id)
    left join vk_data.resources.us_cities 
        on lower(rtrim(ltrim(customer_address.customer_state))) = lower(trim(us_cities.state_abbr))
        and trim(lower(customer_address.customer_city)) = trim(lower(us_cities.city_name))
)

, food_preferences as (
    select 
        customer_id
        , count(*) as food_pref_count
    from vk_data.customers.customer_survey
    where is_active = true
    group by 1
) 

, chicago_store as (
    select 
        geo_location
    from vk_data.resources.us_cities 
    where city_name = 'CHICAGO' and state_abbr = 'IL'
)

, gary_store as (
    select 
        geo_location
    from vk_data.resources.us_cities 
    where city_name = 'GARY' and state_abbr = 'IN'
)

select 
    customers.customer_name
    , customers.customer_city
    , customers.customer_state
    , food_preferences.food_pref_count
    , (st_distance(customers.geo_location, chicago_store.geo_location) / 1609)::int as chicago_distance_miles
    , (st_distance(customers.geo_location, gary_store.geo_location) / 1609)::int as gary_distance_miles
from customers
inner join food_preferences using(customer_id)
cross join chicago_store
cross join gary_store
where 
    (
       customer_state = 'KY'
       and (trim(customer_city) ilike '%concord%' 
       or trim(customer_city) ilike '%georgetown%' 
       or trim(customer_city) ilike '%ashland%')

    )
    or
    (
       customer_state = 'CA' 
       and (trim(customer_city) ilike '%oakland%' 
       or trim(customer_city) ilike '%pleasant hill%')
    )
    or
    (   
       customer_state = 'TX' 
       and (trim(customer_city) ilike '%arlington%') 
       or trim(customer_city) ilike '%brownsville%'
    )
    and chicago_distance_miles BETWEEN 1 and 10000