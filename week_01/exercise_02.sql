-- eligible_customer CTE:  These are the customers who are eligible to order from Virtual Kitchen. The cities in the City table have also been deduplicated
-- customer_base CTE: These are the customers who are eligible to order AND have at least one food preference selected. This would be our base customers for sending personalized email messages
-- food_preference CTE: The food preferences are pivoted from rows to columns. Only the first 3 tags (sorting by alphabetical) are pivoted
-- one_tag_per_recipe CTE: The recipe tags are flatten from the Recipe table. We take one tag per recipe
-- final transformation: The last 2 CTEs are joined together to retrieve one recipe that matches food preference #1 of our customers
with eligible_customer as (
    select 
        c.customer_id
    from customers.customer_address c
        inner join (
                    select *
                    from resources.us_cities
                    qualify row_number() over (partition by city_name, state_abbr order by county_name asc)=1
                  ) u on upper(trim(city_name)) = upper(trim(customer_city)) and upper(trim(state_abbr)) = upper(trim(customer_state))
)

, customer_base as (
    select 
        customer_id
        , tag_property
        , row_number() over (partition by c.customer_id order by tag_property asc) as food_preference
    from customers.customer_survey c
        inner join resources.recipe_tags r using (tag_id)
        inner join eligible_customer using (customer_id)
    where c.is_active = true
)

, food_preference as (
    select *
    from customer_base
    pivot(max(tag_property) 
          for food_preference in (1, 2, 3))
          as pivot_values (customer_id, food_pref_1, food_pref_2, food_pref_3)
)

, one_tag_per_recipe as (
    select 
        recipe_tag
        , max(recipe_name) as suggested_recipe
    from (
          select 
             recipe_name
             , trim(replace(flat_tag.value, '"', '')) as recipe_tag
          from chefs.recipe
          , table(flatten(tag_list)) as flat_tag
         ) as recipe 
    group by 1
)

select 
    f.customer_id
    , c.email
    , c.first_name
    , f.food_pref_1
    , f.food_pref_2
    , f.food_pref_3
    , r.suggested_recipe
from food_preference f 
    left join one_tag_per_recipe r on r.recipe_tag = f.food_pref_1
    left join customers.customer_data c using (customer_id)
order by 2 asc