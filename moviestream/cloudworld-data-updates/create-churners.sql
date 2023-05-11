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

-- Recreate custsales
drop table custsales;
create table custsales 
partition by range (day_id)
interval(numtoyminterval(1, 'MONTH'))
( 
   partition p1 values less than (to_date('01-01-2019', 'DD-MM-YYYY'))
)
as 
select * 
from custsales_original
; 
alter table custsales enable row movement;

-- Table used for computing
drop table cust_scratch;
create or replace view v_cust_scratch as
    with base as (
        select 
            c.cust_id,
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
            1 as views_total,           
            case when to_char(cs.day_id, 'YYYY-MM') = '2020-12' then 1 else 0 end as views_m1,
            case when to_char(cs.day_id, 'YYYY-MM') = '2020-11' then 1 else 0 end as views_m2,
            case when to_char(cs.day_id, 'YYYY-MM') = '2020-10' then 1 else 0 end as views_m3,
            case when to_char(cs.day_id, 'YYYY-MM') = '2020-09' then 1 else 0 end as views_m4,
            case when to_char(cs.day_id, 'YYYY-MM') = '2020-08' then 1 else 0 end as views_m5,
            case when to_char(cs.day_id, 'YYYY-MM') = '2020-07' then 1 else 0 end as views_m6,
            case when to_char(cs.day_id, 'YYYY-MM') = '2020-06' then 1 else 0 end as views_m7,
            case when to_char(cs.day_id, 'YYYY-MM') = '2020-05' then 1 else 0 end as views_m8,
            case when to_char(cs.day_id, 'YYYY-MM') = '2020-04' then 1 else 0 end as views_m9,
            case when to_char(cs.day_id, 'YYYY-MM') = '2020-03' then 1 else 0 end as views_m10,
            case when to_char(cs.day_id, 'YYYY-MM') = '2020-02' then 1 else 0 end as views_m11,
            case when to_char(cs.day_id, 'YYYY-MM') = '2020-01' then 1 else 0 end as views_m12,
            case when to_char(cs.day_id, 'YYYY-MM') = '2019-12' then 1 else 0 end as views_m13,
            case when to_char(cs.day_id, 'YYYY-MM') = '2019-11' then 1 else 0 end as views_m14,
            case when cs.actual_price > 0  then 1 else 0 end as paid_total,
            case when to_char(cs.day_id, 'YYYY-MM') = '2020-12' and cs.actual_price > 0 then 1 else 0 end as paid_m1,
            case when to_char(cs.day_id, 'YYYY-MM') = '2020-11' and cs.actual_price > 0 then 1 else 0 end as paid_m2,
            case when to_char(cs.day_id, 'YYYY-MM') = '2020-10' and cs.actual_price > 0 then 1 else 0 end as paid_m3,
            case when to_char(cs.day_id, 'YYYY-MM') = '2020-09' and cs.actual_price > 0 then 1 else 0 end as paid_m4,
            case when to_char(cs.day_id, 'YYYY-MM') = '2020-08' and cs.actual_price > 0 then 1 else 0 end as paid_m5,
            case when to_char(cs.day_id, 'YYYY-MM') = '2020-07' and cs.actual_price > 0 then 1 else 0 end as paid_m6,                            
            case when cs.discount_percent > 0  then 1 else 0 end as discount_total,
            case when to_char(cs.day_id, 'YYYY-MM') = '2020-12' and cs.discount_percent > 0 then 1 else 0 end as discount_m1,
            case when to_char(cs.day_id, 'YYYY-MM') = '2020-11' and cs.discount_percent > 0 then 1 else 0 end as discount_m2,
            case when to_char(cs.day_id, 'YYYY-MM') = '2020-10' and cs.discount_percent > 0 then 1 else 0 end as discount_m3,
            case when to_char(cs.day_id, 'YYYY-MM') = '2020-09' and cs.discount_percent > 0 then 1 else 0 end as discount_m4,
            case when to_char(cs.day_id, 'YYYY-MM') = '2020-08' and cs.discount_percent > 0 then 1 else 0 end as discount_m5,
            case when to_char(cs.day_id, 'YYYY-MM') = '2020-07' and cs.discount_percent > 0 then 1 else 0 end as discount_m6,            
            case when genre_id in (1,2,6,20,22) then 1 else 0 end as action_adventure,
            case when genre_id in (3,9,15) then 1 else 0 end as family_friendly,
            case when genre_id in (6,7,13) then 1 else 0 end as docu_crime,
            case when genre_id in (8,19)   then 1  else 0 end as drama_romance,
            case when genre_id=1 then 1 else 0 end as action,
            case when genre_id=2 then 1 else 0 end as adventure,
            case when genre_id=3 then 1 else 0 end as animation,
            case when genre_id=4 then 1 else 0 end as biography,
            case when genre_id=5 then 1 else 0 end as comedy,
            case when genre_id=6 then 1 else 0 end as crime,
            case when genre_id=7 then 1 else 0 end as documentary,
            case when genre_id=8 then 1 else 0 end as drama,
            case when genre_id=9 then 1 else 0 end as family,
            case when genre_id=10 then 1 else 0 end as fantasy,
            case when genre_id=11 then 1 else 0 end as film_noir,
            case when genre_id=12 then 1 else 0 end as history,
            case when genre_id=13 then 1 else 0 end as horror,
            case when genre_id=14 then 1 else 0 end as lifestyle,
            case when genre_id=15 then 1 else 0 end as musical,
            case when genre_id=16 then 1 else 0 end as mystery,
            case when genre_id=17 then 1 else 0 end as news,
            case when genre_id=18 then 1 else 0 end as reality_tv,
            case when genre_id=19 then 1 else 0 end as romance,
            case when genre_id=20 then 1 else 0 end as sci_fi,
            case when genre_id=21 then 1 else 0 end as sport,
            case when genre_id=22 then 1 else 0 end as thriller,
            case when genre_id=23 then 1 else 0 end as war,
            case when genre_id=24 then 1 else 0 end as western                            
        from customer c, custsales cs, customer_contact cc, customer_segment s
        where c.cust_id = cs.cust_id
        and c.cust_id = cc.cust_id
        and c.segment_id = s.segment_id
    ),
    agg_base as (
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
            sum(views_total) as views_total,
            sum(views_m1) as views_m1,
            sum(views_m2) as views_m2,
            sum(views_m3) as views_m3,
            sum(views_m4) as views_m4,
            sum(views_m5) as views_m5,
            sum(views_m6) as views_m6,    
            sum(views_m7) as views_m7,
            sum(views_m8) as views_m8,
            sum(paid_total) as paid_total,
            sum(paid_m1) as paid_m1,
            sum(paid_m2) as paid_m2,
            sum(paid_m3) as paid_m3,
            sum(paid_m4) as paid_m4,
            sum(paid_m5) as paid_m5,
            sum(paid_m6) as paid_m6,
            sum(discount_total) as discount_total,
            sum(discount_m1) as discount_m1,
            sum(discount_m2) as discount_m2,
            sum(discount_m3) as discount_m3,
            sum(discount_m4) as discount_m4,
            sum(discount_m5) as discount_m5,
            sum(discount_m6) as discount_m6,
            sum(action_adventure) as action_adventure,
            sum(family_friendly) as family_friendly,
            sum(docu_crime) as docu_crime,
            sum(drama_romance) as drama_romance,
            sum(action) as action,
            sum(adventure) as adventure,
            sum(animation) as animation,
            sum(biography) as biography,
            sum(comedy) as comedy,
            sum(crime) as crime,
            sum(documentary) as documentary,
            sum(drama) as drama,
            sum(family) as family,
            sum(fantasy) as fantasy,
            sum(film_noir) as film_noir,
            sum(history) as history,
            sum(horror) as horror,
            sum(lifestyle) as lifestyle,
            sum(musical) as musical,
            sum(mystery) as mystery,
            sum(news) as news,
            sum(reality_tv) as reality_tv,
            sum(romance) as romance,
            sum(sci_fi) as sci_fi,
            sum(sport) as sport,
            sum(thriller) as thriller,
            sum(war) as war,
            sum(western) as western
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
    ),
    add_calcs as (
        select 
            agg_base.*,
            round(views_total/24, 2) as views_monthly_avg,        
            round(views_m1/nullif(views_m2,0), 2) as views_m1_pr_month_ratio,    
            round(views_m2/nullif(views_m3,0), 2) as views_m2_pr_month_ratio,
            round(views_m3/nullif(views_m4,0), 2) as views_m3_pr_month_ratio,
            case  
                when (paid_m1 + paid_m2 + paid_m3) / nullif((views_m1 + views_m2 + views_m3),0) - (paid_total) / nullif((views_total),0) > 0 then 'up'
                when (paid_m1 + paid_m2 + paid_m3) / nullif((views_m1 + views_m2 + views_m3),0) - (paid_total) / nullif((views_total),0) < 0 then 'down'
                else 'flat'
            end paid_ratio_trend,       
            case  
                when (discount_m1 + discount_m2 + discount_m3) / nullif((views_m1 + views_m2 + views_m3),0) - (discount_total) / nullif((views_total),0) > 0 then 'up'
                when (discount_m1 + discount_m2 + discount_m3) / nullif((views_m1 + views_m2 + views_m3),0) - (discount_total) / nullif((views_total),0) < 0 then 'down'
                else 'flat'
            end discount_ratio_trend
        from agg_base
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
    views_total,
    views_m3,
    views_m4,
    views_m5,
    views_m6,
    views_monthly_avg,
    views_m3_pr_month_ratio,
    paid_total,
    paid_ratio_trend,
    paid_m3,
    paid_m4,
    paid_m5,
    paid_m6,
    discount_total,
    discount_ratio_trend,
    discount_m3,
    discount_m4,
    discount_m5,
    discount_m6,
    action_adventure,
    family_friendly,
    docu_crime,
    drama_romance,
    action,
    adventure,
    animation,
    biography,
    comedy,
    crime,
    documentary,
    drama,
    family,
    fantasy,
    film_noir,
    history,
    horror,
    lifestyle,
    musical,
    mystery,
    news,
    reality_tv,
    romance,
    sci_fi,
    sport,
    thriller,
    war,
    western
from add_calcs;

drop table cust_scratch;
create table cust_scratch as select * from v_cust_scratch;

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



-- ## CREATE CHURNERS ## --
-- November - views=0
-- October - decrease # views
-- October - increase # paid views
-- October -- decrease # discounts

-- Find the nov churners. Take about 1/3 of those who meet the profile; Then, add a small percent.
drop table all_churners;
create table all_churners
as 
WITH profile as (
    select cust_id,
    11 as month
    FROM
        cust_scratch c    
    WHERE
        c.age between 28 and 40
    and c.yrs_current_employer < 4
    and c.work_experience > 5
    and c.yrs_residence < 6
    and c.insuff_funds_incidents > 0
    and c.action_adventure > 25
)
select *
from profile sample(33);

insert into all_churners(cust_id, month)
select cust_id, 11
from cust_scratch sample(.2)
where cust_id not in (select cust_id from all_churners where month=11);
commit;
select count(*) from all_churners;

-- Move their purchases back 3 months
update custsales
set day_id = add_months(day_id, -4)
where cust_id in (select cust_id from all_churners where month=11)
and to_char(day_id, 'YYYY-MM') in ('2020-12','2020-11');
commit;

-- Do the same for december. Picking a higher sample b/c a set of churners
-- were already selected in Nov - so the potential pool has shrunk
insert into all_churners(cust_id, month)
WITH profile as (
    select cust_id,
    12 as month
    FROM
        cust_scratch c    
    WHERE
        c.age between 28 and 40
    and c.yrs_current_employer < 4
    and c.work_experience > 5
    and c.yrs_residence < 6
    and c.insuff_funds_incidents > 0
    and c.action_adventure > 25
    and cust_id not in (select cust_id from all_churners where month=11)
)
select cust_id, month
from profile sample(70);

commit;
insert into all_churners(cust_id, month)
select cust_id, 12
from cust_scratch sample(.2)
where cust_id not in (select cust_id from all_churners)
;
commit;

select count(*) from all_churners;

-- Move their purchases back several months
update custsales
set day_id = add_months(day_id, -5)
where cust_id in (select cust_id from all_churners where month=12)
and to_char(day_id, 'YYYY-MM') in ('2020-12');
commit;


-- Reduce the discount churners were receiving starting in August
drop table cust_remove_discount;
create table cust_remove_discount as select cust_id from all_churners sample(70);

update custsales
set discount_percent = 0, 
    discount_type = 'none',
    actual_price = list_price - (list_price * discount_percent)
where cust_id in (select cust_id from cust_remove_discount)
and day_id >= to_date('2020-08', 'YYYY-MM');
commit;

select discount_type, discount_percent from custsales where discount_percent=0;
commit;

-- recreate cust_scratch from the v_cust_scratch b/c data in the fact has been updated

drop table cust_scratch;
create table cust_scratch as select * from v_cust_scratch;

select count(*)
from moviestream_churn_original 
where is_churner=1;

select count(*)
from cust_scratch sample(4)
where cust_id not in (select cust_id from all_churners);

-- moviestream_churn table will only have a subset of customers.
-- add customers from both the churners and the non-churners
drop table churn_sample;
create table churn_sample as
    select cust_id
    from all_churners sample(12)
    union all
    select cust_id
    from cust_scratch sample(4)
    where cust_id not in (select cust_id from all_churners);


drop table moviestream_churn;
create table moviestream_churn as
select 
    0 as is_churner,
    c.*
from v_cust_scratch c
where c.cust_id in (select cust_id from churn_sample);
    
select count(*)
from moviestream_churn;

update moviestream_churn
set is_churner=1
where cust_id in (select cust_id from all_churners);
commit;

select count(*) 
from moviestream_churn
where is_churner=1;


