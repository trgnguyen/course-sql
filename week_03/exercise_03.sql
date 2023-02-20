/* We want to create a daily report to track:
1) Total unique sessions
2) The average length of sessions in seconds
3) The average number of searches completed before displaying a recipe 
4) The ID of the recipe that was most viewed */
with events_recipes as (
    select
        event_id
        , session_id
        , event_timestamp
        , trim(parse_json(event_details):"recipe_id")::string as recipe_id
        , trim(parse_json(event_details):"event")::string as event_activity
    from events.website_activity
    group by 1,2,3,4,5
)

, session as (
	select 
    	session_id 
        , min(event_timestamp) as min_event_timestamp
        , datediff(second,min(event_timestamp), max(event_timestamp)) as session_duration
        , count_if(event_activity='search') as search_cnt
    from events_recipes
    group by 1
)

, search_event as (
	select 
    	session_id 
        , count_if(event_activity='search') as search_cnt
    from events_recipes
    group by 1
)

, recipe_views as (
    select 
    	date(event_timestamp) as event_date
        , recipe_id
        , count(*) as total_views
    from events_recipes
    where recipe_id is not null
    group by 1,2
    qualify row_number() over(partition by event_date order by total_views desc)=1
)

select
	date(min_event_timestamp) as event_date
    , count(session_id) as total_sessions
    , avg(session_duration) as avg_session_duration
    , avg(search_cnt) as avg_search
    , max(recipe_id) as top_recipe
from session
inner join recipe_views on date(session.min_event_timestamp) = recipe_views.event_date
group by 1