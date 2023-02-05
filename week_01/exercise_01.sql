-- us_cities CTE: The cities in the City table have been deduplicated by using qualify row_number=1
-- customer_location CTE: These are the customers who are eligible to order. Out of 10K customers, 2,401 customers are eligible to order from Virtual Kitchen. An inner join was performed between the Customer table with the City table to retrieve the geo_location information 
-- supplier_location CTE: An inner join was performed between the Supplier table with the City table to retrieve the geo_location information
-- closest_distance_to_supplier CTE: A cross join was performed between the Customer table and the Supplier table to calculate the distance. We then set row_number=1 to choose the closest distance between the customers and the supplier
-- final transformation: A simple join to get  our customer data, to order the results by the customer’s last name and first name, and to have the table in the final format

with us_cities as (
    select *
    from resources.us_cities
    qualify row_number() over (partition by city_name, state_abbr order by county_name asc)=1
)

, customer_location as (
    select 
        c.*
        , u.geo_location
    from customers.customer_address c
        inner join us_cities u on upper(trim(city_name)) = upper(trim(customer_city)) and upper(trim(state_abbr)) = upper(trim(customer_state))
)

, supplier_location as (
    select 
        s.*
        , u.geo_location
    from suppliers.supplier_info s
        inner join us_cities u on upper(trim(city_name)) = upper(trim(supplier_city)) and upper(trim(state_abbr)) = upper(trim(supplier_state))
)

, closest_distance_to_supplier as (
    select 
        *
        , (st_distance (s.geo_location, c.geo_location))/1000 as distance_in_kilometers
    from customer_location c 
        cross join supplier_location s
    qualify row_number() over (partition by customer_id order by st_distance (s.geo_location, c.geo_location) asc )=1
) 

select 
    c.customer_id
    , d.first_name
    , d.last_name
    , d.email
    , c.supplier_id
    , c.supplier_name
    , c.distance_in_kilometers
from closest_distance_to_supplier c 
    left join customers.customer_data d using (customer_id)
order by 3,2