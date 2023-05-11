create table sales_dashboard as
select
    cc.last_name,
    cc.first_name,
    cs.short_name as customer_segment,
    ce.education,
    ce.gender,
    ce.household_size,
    ce.job_type,
    ce.income_level,
    case 
        when ce.age > 75 then 'Silent Generation'
        when ce.age between 57 and 75 then 'Boomer'
        when ce.age between 41 and 56 then 'Gen X'
        when ce.age between 25 and 40 then 'Millenials'
        when ce.age between 9 and 24 then 'Gen Z'
        end as age_range,
    cc.country,
    cc.city,
    c.day_id,
    g.name genre,
    m.title,
    m.year,
    translate(m.cast,'[]"','  ') as cast,
    nvl(regexp_count(m.awards,'Academy Award'),0) as academy_awards,
    nvl(regexp_count(m.nominations,'Academy Award'),0) as academy_nominations,
    1 as views,
    actual_price as sales        
from dcat$obj_landing.customer_extension ce, custsales c, genre g, dcat$obj_landing.customer_segment cs, customer_contact cc, movie m
where ce.cust_id = c.cust_id
  and ce.cust_id = cc.cust_id
  and g.genre_id = c.genre_id
  and ce.segment_id = cs.segment_id
  and c.movie_id = m.movie_id
 ;