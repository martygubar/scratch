/*
Will create:
1. Reset custsales from the original
2. Create the CUST_SCRATCH table used for generating the data
3. ALL_CHURNERS table. This is all of the customers that have churned in Nov or Dec
   It includes the cust_id and the month that they left
4. Shift there views back a few months
5. Add a sampling of others that will also churn (outside the profile)
6. Create the moviestream_churn table
    - it is a small subset of the entire customer data set
    - it includes customers that will and will not churn
    - there will be many customers outside of this table that have churned. The model can be
      applied to them if desicred.

Profile of churners:
* We are losing young customers that watch action adventure movies
* They pay a subscription and don't nec want to pay any more than that 
* Their discount ratio may be declining and their paid ratio is increasing
* Need to reconsider how to keep them
* There is also a geographic clustering


Will want to tweak these stats. Perhaps lower yrs_residence and yrs_current_employer
    age between 28 and 40
    yrs_current_employer < 4
    work_experience > 5
    yrs_residence < 6
    insuff_funds_incidents > 0
    action_adventure > 25

To do:
* paid ratio - start charging for more action adventure movies?
* lower yrs_current_employer for many
* create geographic cluster

*/

select *
from genre_movie;

-- Here's the an example
SELECT 
    cust_id,
    paid_ratio_trend,
    discount_ratio_trend,
    city,
    state_province,
    country,
    loc_lat,
    loc_long,
    views_total,
    views_m1, --dec
    views_m2, --nov
    views_m3, --oct
    views_m4, --sep
    views_m5  --aug
FROM
    cust_scratch c    
WHERE
    c.age between 28 and 40
and c.yrs_current_employer < 4
and c.work_experience > 5
and c.yrs_residence < 6
and c.insuff_funds_incidents > 0
and c.action_adventure > 25
;

-- Below is the dominant customer profile of people that are at-risk
SELECT 
    count(*)
FROM
    cust_scratch c    
WHERE
    c.age between 28 and 40
and c.yrs_current_employer < 4
and c.work_experience > 5
and c.yrs_residence < 6
and c.insuff_funds_incidents > 0
and c.action_adventure > 25
;


select count(*) from all_churners;

select discount_type, discount_percent from custsales where discount_percent=0;


select count(*)
from moviestream_churn_original 
where is_churner=1;

select count(*)
from cust_scratch sample(4)
where cust_id not in (select cust_id from all_churners);
    
select count(*)
from moviestream_churn;

select count(*) 
from moviestream_churn
where is_churner=1;

select trunc(dbms_random.value(3,10))
from dual;

select count(*)
from genre_movie;

select *
from genre_movie sample(1);

select * 
from genre_movie 
order by dbms_random.value 
fetch first 1 rows only;
 
 
select null+1 from dual; 

select views_m3, paid_m3, discount_m3 from moviestream_churn where cust_id=1304758;

select *
from custsales
where cust_id=1304758
order by day_id desc;

select * from genre;

select title, name, genre, g.genre_id, m.movie_id
from custsales_shift_movies g, movie m, genre ge
where m.movie_id = g.movie_id
and ge.genre_id = g.genre_id
;


SELECT a.cust_id, b.chain, b.address, b.city, b.state,
       round( sdo_nn_distance(1), 1 ) distance_km
FROM customer_contact a, pizza_location b
WHERE a.state_province = 'Rhode Island'
AND sdo_nn(
     latlon_to_geometry(b.lat, b.lon),
     latlon_to_geometry(a.loc_lat, a.loc_long),
     'sdo_num_res=1 distance=10 unit=KM',
     1 ) = 'TRUE';

SELECT a.cust_id, a.city, b.cust_id, b.city, b.state,
       round( sdo_nn_distance(1), 1 ) distance_km
FROM cust_scratch a, all_churners b
WHERE a.state_province = 'Rhode Island'
AND sdo_nn(
     latlon_to_geometry(b.lat, b.lon),
     latlon_to_geometry(a.loc_lat, a.loc_long),
     'sdo_num_res=1 distance=10 unit=KM',
     1 ) = 'TRUE';
     
     
select is_churner, discount_ratio_trend
from moviestream_churn
;