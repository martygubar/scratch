--==============================
-- Start from base
-- Load clean movies
-- Drop transactions for bad movies
--==============================
begin
    dbms_cloud.create_external_table
    (
      table_name => 'ext_custsales',
      file_uri_list => 'https://objectstorage.us-ashburn-1.oraclecloud.com/n/c4u04/b/moviestream_gold/o/custsales/*.parquet',
      format => '{"type":"parquet", "schema": "first"}'

    );
end;
/

create table custsales_original as select * from ext_custsales;

-- create clean movie collection 
-- create and load movie json collection from a public bucket on object storage

declare 
    i number;
begin
    i := DBMS_SODA.drop_collection('MOVIE_COLLECTION');

    dbms_cloud.copy_collection (
        collection_name => 'MOVIE_COLLECTION',
        file_uri_list   => 'https://objectstorage.us-sanjose-1.oraclecloud.com/n/adwc4pm/b/moviestream_gold/o/movie/movies.json',    
        format          => '{ignoreblanklines:true}'
    );
end;
/


-- Create a view over the collection to make queries easy
drop table movie;
create table movie as
select
    json_value(json_document, '$.movie_id' returning number) as movie_id,
    json_value(json_document, '$.title') as title,
    json_value(json_document, '$.budget' returning number) as budget,
    json_value(json_document, '$.list_price' returning number) as list_price,
    json_value(json_document, '$.gross' returning number) as gross,
    json_query(json_document, '$.genre' returning varchar2(400)) as genre,
    json_value(json_document, '$.sku' returning varchar2(30)) as sku,
    json_value(json_document, '$.year' returning number) as year, 
    json_value(json_document, '$.opening_date' returning date) as opening_date,
    json_value(json_document, '$.views' returning number) as views, 
    json_query(json_document, '$.cast' returning varchar2(4000)) as cast,
    json_query(json_document, '$.crew' returning varchar2(4000)) as crew,
    json_query(json_document, '$.studio' returning varchar2(4000)) as studio,
    json_value(json_document, '$.main_subject' returning varchar2(400)) as main_subject,
    json_query(json_document, '$.awards' returning varchar2(4000)) as awards,
    json_query(json_document, '$.nominations' returning varchar2(4000)) as nominations,
    json_value(json_document, '$.runtime' returning number) as runtime, 
    json_value(json_document, '$.summary' returning varchar2(10000)) as summary 
from movie_collection
;

-- Create custsales with only valid movies
drop table custsales;
create table custsales as
select *
from custsales_original
where movie_id in (select movie_id from movie);


-- Create customer table with views and sales
drop table cust_scratch;
create table cust_scratch as
with base as (
    select c.cust_id,
           c.last_name,
           c.first_name,
           c.city,
           c.state_province,
           c.country,
           c.loc_lat,
           c.loc_long,  
           c.age,
           case 
            when c.age > 75 then 'Silent Generation'
            when c.age between 57 and 75 then 'Boomer'
            when c.age between 41 and 56 then 'Gen X'
            when c.age between 25 and 40 then 'Millenials'
            when c.age between 9 and 24 then 'Gen Z'
            end as age_range,
           c.education,
           c.full_time,
           c.gender,
           c.household_size,
           c.income,
           c.insuff_funds_incidents,
           c.job_type,
           c.rent_own,
           c.pet,
           c.work_experience,
           c.yrs_current_employer,
           c.yrs_residence,
           s.short_name as customer_segment,       
           1 as views,
           case when cs.day_id > to_date('2020-09', 'YYYY-MM') then 1 
                else null
                end as views_3_latest_months,  
           case when cs.day_id < to_date('2020-10', 'YYYY-MM') then 1 
                else null
                end as views_pre_oct,                            
           case when to_char(cs.day_id, 'YYYY-MM') = '2020-12' then 1 
                else null
                end as views_dec,  
           case when to_char(cs.day_id, 'YYYY-MM') = '2020-11' then 1 
                else null
                end as views_nov,  
           case when to_char(cs.day_id, 'YYYY-MM') = '2020-10' then 1 
                else null
                end as views_oct, 
           case when genre_id in (1,2,6,20,22) then 1 
                else null
                end as action_adventure,
           case when genre_id in (3,9,15) then 1 
                else null
                end as family_friendly,
           case when genre_id in (6,7,13) then 1 
                else null
                end as docu_crime,
           case when genre_id in (8,19) then 1 
                else null
                end as drama_romance            
    from customer c, custsales cs, customer_contact cc, customer_segment s
    where c.cust_id = cs.cust_id
    and c.cust_id = cc.cust_id
    and c.segment_id = s.segment_id
)
select
    cust_id,
    last_name,
    first_name,
    city,
    state_province,
    country,
    loc_lat,
    loc_long,
    age,
    age_range,
    education,
    full_time,
    gender,
    household_size,
    income,
    insuff_funds_incidents,
    job_type,
    rent_own,
    pet,
    work_experience,
    yrs_current_employer,
    yrs_residence,
    customer_segment,
    sum(views) as views,
    sum(views_dec) as views_dec,
    sum(views_nov) as views_nov,
    sum(views_oct) as views_oct,
    sum(views_3_latest_months) as views_3_latest_months,
    round(sum(views_pre_oct)/21, 0) as views_pre_oct_avg,
    sum(action_adventure) as action_adventure,
    sum(family_friendly) as family_friendly,
    sum(docu_crime) as docu_crime,
    sum(drama_romance) as drama_romance
from
    base
group by 
    cust_id,
    last_name,
    first_name,
    city,
    state_province,
    country,
    loc_lat,
    loc_long,
    age,
    age_range,
    education,
    full_time,
    gender,
    household_size,
    income,
    insuff_funds_incidents,
    job_type,
    rent_own,
    pet,
    work_experience,
    yrs_current_employer,
    yrs_residence,
    customer_segment
    ;

-- Find churners
drop table new_churner;
create table new_churner as 
WITH base as (
SELECT
    cust_id,
    city,
    state_province,
    country,
    loc_lat,
    loc_long,
    views,
    views_dec,
    views_nov,
    views_oct,
    views_3_latest_months,
    views_pre_oct_avg,
    action_adventure,
    family_friendly,
    docu_crime,
    drama_romance
FROM
    cust_scratch c    
WHERE
    c.age between 28 and 40
and c.yrs_current_employer < 4
and c.yrs_residence < 6
and c.insuff_funds_incidents > 0
and c.action_adventure > 25)
select *
from base sample(50)
;

-- Who is not a churner? Create a view
create or replace view not_churner as
select cust_id,
    city,
    state_province,
    country,
    loc_lat,
    loc_long,
    views,
    views_dec,
    views_nov,
    views_oct,
    views_3_latest_months,
    views_pre_oct_avg,
    action_adventure,
    family_friendly,
    docu_crime,
    drama_romance
FROM
    cust_scratch c    
where c.cust_id not in (select n.cust_id from new_churner n);

-- Add some churners based on their location compared to existing churners
-- Need spatial indexes and metadata
insert into user_sdo_geom_metadata values (
 'CUST_SCRATCH',
 user||'.LATLON_TO_GEOMETRY(loc_lat,loc_long)',
  sdo_dim_array(
      sdo_dim_element('X', -180, 180, 0.05), --longitude bounds and tolerance in meters
      sdo_dim_element('Y', -90, 90, 0.05)),  --latitude bounds and tolerance in meters
  4326 --identifier for lat/lon coordinate system
    );

insert into user_sdo_geom_metadata values (
 'NEW_CHURNER',
 user||'.LATLON_TO_GEOMETRY(loc_lat,loc_long)',
  sdo_dim_array(
      sdo_dim_element('X', -180, 180, 0.05), --longitude bounds and tolerance in meters
      sdo_dim_element('Y', -90, 90, 0.05)),  --latitude bounds and tolerance in meters
  4326 --identifier for lat/lon coordinate system
);
    
commit;

CREATE INDEX cust_scratch_sidx ON cust_scratch (latlon_to_geometry(loc_lat,loc_long)) INDEXTYPE IS mdsys.spatial_index_v2 PARAMETERS ('layer_gtype=POINT');
CREATE INDEX new_churner_sidx ON new_churner (latlon_to_geometry(loc_lat,loc_long)) INDEXTYPE IS mdsys.spatial_index_v2 PARAMETERS ('layer_gtype=POINT');

-- Let's now find customers that are near these churners



-- For these customers
-- Clear out their december views (shift back 2 months)
-- Move a random sampling of their november sales back 2 months
drop table new_churners;

create table new_churners as 
select cust_id, 
       round(dbms_random.value(0,1),0) as nov_slowdown
from cust_scratch c
where   c.age between 28 and 40
    and c.work_experience > 5
    and c.yrs_current_employer < 4
    and c.yrs_residence < 6
    and c.insuff_funds_incidents > 0
    and c.action_adventure > 25
;

select count(*) from new_churners;
select count(*) from customer;

-- move december
update custsales
set day_id = day_id - 60
where cust_id in (select cust_id 
                  from new_churners
                  where to_char(day_id, 'YYYY-MM') = '2020-12'
                  );
commit;

-- move a sample of november back
update custsales
set day_id = day_id - 60
where cust_id in (select cust_id 
                  from new_churners
                  where nov_slowdown = 1
                  );
commit;



---
--- APPENDIX
---
-- Customers who are near other potential churners
select count(*),
       trunc(avg(views)) as avg_views,
       min(views),
       max(views),
       median(views),
       trunc(avg(recent_views)) as avg_recent_views,
       min(recent_views),
       max(recent_views),
       count(action_adventure),
       trunc(avg(action_adventure)) as avg_action_adventure,       
       count(family_friendly),
       trunc(avg(family_friendly)) as avg_family_friendly,       
       count(docu_crime),
       trunc(avg(docu_crime)) as avg_docu_crime,       
       count(family_friendly),       
       trunc(avg(family_friendly)) as avg_action_adventure,       
       count(drama_romance),
       trunc(avg(drama_romance)) as avg_drama_romance
from (
select 
    c.last_name,
    c.age,
    c.work_experience,
    c.yrs_current_employer,
    c.yrs_residence,
    c.rent_own,
    c.insuff_funds_incidents,
    c.views,
    c.recent_views,
    c.action_adventure,
    c.family_friendly,
    c.docu_crime,
    c.drama_romance
from cust_scratch c, cust_scratch cs
where 
    c.age between 30 and 40
and c.work_experience > 6
and c.yrs_current_employer < 4
and c.yrs_residence < 6
and c.insuff_funds_incidents > 0
and sdo_within_distance(
      latlon_to_geometry(c.loc_lat, c.loc_long),
      sdo_geometry(2001, 4326, sdo_point_type(cs.loc_long, cs.loc_lat, null),null, null),
      'distance=100 unit=mile') = 'TRUE'
);



