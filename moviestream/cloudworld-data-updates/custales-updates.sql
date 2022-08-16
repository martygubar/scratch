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
create table cust_scratch as
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
            end) as recent_views
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
       s.short_name;

-- 