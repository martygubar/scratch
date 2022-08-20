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
        file_uri_list   => 'https://objectstorage.sa-saopaulo-1.oraclecloud.com/n/adwc4pm/b/moviestream_gold/o/movie/movies.json',    
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

-- Start tweaking data with following rules:
-- Story:
--Age range
--work_experience >> longer
--yrs_current_employer >> shorter
--yrs_residence >> lower
--rent_own --> renter
--Insufficient funds is higher
--Within x miles of major city
--
--x group slowed usage in October
--x group leave in November
--Y Start to slow down in nov, dec

-- Create customer table with views and sales

-- Add spatial???
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
           count(*) as views,
           sum(case when cs.day_id > to_date('2020-09', 'YYYY-MM') then 1 
                else null
                end) as recent_views,
           sum(case when genre_id in (1,2,6,20,22) then 1 
                else null
                end) as action_adventure,
           sum(case when genre_id in (3,9,15) then 1 
                else null
                end) as family_friendly,
           sum(case when genre_id in (6,7,13) then 1 
                else null
                end) as docu_crime,
           sum(case when genre_id in (8,19) then 1 
                else null
                end) as drama_romance            
    from customer c, custsales cs, customer_contact cc, customer_segment s
    where c.cust_id = cs.cust_id
    and c.cust_id = cc.cust_id
    and c.segment_id = s.segment_id
    group by c.cust_id,
           c.last_name,
           c.first_name,
           c.city,
           c.state_province,
           c.country,
           c.loc_lat,
           c.loc_long,
           c.age,
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
           s.short_name
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
    views,
    recent_views,
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
    views,
    recent_views
    ;


-- Take a look at the data
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
from cust_scratch c
where 
    c.age between 28 and 40
and c.work_experience > 5
and c.yrs_current_employer < 4
and c.yrs_residence < 6
and c.insuff_funds_incidents > 0
and c.action_adventure > 25

);

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




select * from major_city; 

create table major_city as
select distinct(city) as city, loc_lat, loc_long 
from customer
where city in ('Omaha','Lima', 'Boston');

select distinct c.last_name, c.city
from customer c, major_city m
where sdo_within_distance(
 latlon_to_geometry(c.loc_lat, c.loc_long),
 sdo_geometry(2001, 4326, sdo_point_type(m.loc_long, m.loc_lat, null),null, null),
 'distance=100 unit=mile') = 'TRUE';



select * from major_city;
insert into major_city values ('New York',40.73993,-73.76961);
truncate table major_city;
commit;

select city, state_province, loc_long, loc_lat from customer where city in ('Hartford', 'New York');

select * from MDSYS.SDO_DIST_UNITS where upper (UNIT_NAME) like '%MILE%';
