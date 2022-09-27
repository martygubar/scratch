-- queries to get jobs and names out of the array
select 
    json_query(m.crew, '$[*]?(@.job=="producer").names'),
    json_query(m.crew, '$[*].job' with array wrapper),  --> with array wrapper requred otherwise null
    crew
from movietab m;

-- distinct jobs in the movie table crew column
with a as (
select 
    jt.job  as job
from movie m,
    json_table(m.crew, '$[*].job' columns (job path '$')) jt    
order by gross desc nulls last
)
select distinct job 
from a;

-- turn the different jobs into columns
select
    movie_id,
    year,
    title,
    translate(genre, '["]', ' ') as genre,
    translate(
        replace(
            json_query(m.crew, '$[*]?(@.job=="producer").names') ||
            json_query(m.crew, '$[*]?(@.job=="director").names') ||       
            json_query(m.crew, '$[*]?(@.job=="screenwriter").names') ||
            json_query(m.crew, '$[*]?(@.job=="executive producer").names'),
            '][',','),
        '["]', ' ')
            
            as crew, 
    translate(cast, '["]', ' ') as cast,
    summary
from movie m;

-- Use JSON_ARRAYAGG to return similar movies as a single json ARRAY
with similar_movies as (
    select 
        score(1) as score,
        movie_id,
        title
    from movie_similarity 
    where contains(summary, '((2011*.25 OR 2012*.5 OR 2013*1 OR 2014*.5 OR 2015*.2 ) WITHIN year )*.2 
                  ACCUM ( Comedy,Musical,Family,Animation,Fantasy) WITHIN genre  
                  ACCUM ('') WITHIN cast  
                  ACCUM (  Peter Del Vecho,John Lasseter, John Lasseter, Chris Buck,Jennifer Lee, Jennifer Lee) WITHIN crew', 1) > 0 
    and movie_id != 1414
    order by score desc
    fetch first 10 rows only
)
select 
    json_arrayagg (
        json_object(
            'score' value score,
            'movie_id' value movie_id,
            'title' value title
        )    
    )
from similar_movies;