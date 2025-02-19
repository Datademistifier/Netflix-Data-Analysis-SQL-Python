-- Handling foreign characters
-- Using Python

-- Remove duplicates
select show_id,COUNT(*) 
from netflix_raw
group by show_id 
having COUNT(*)>1;

-- Selecting all rows where the combination of uppercase title and type has duplicates
select * from netflix_raw
where concat(upper(title),type)  in (
  select concat(upper(title),type) 
  from netflix_raw
  group by upper(title), type
  having COUNT(*)>1
)
order by title;

-- Using Common Table Expression (CTE) to remove duplicates and get only unique values
with cte as (
  select * 
  ,ROW_NUMBER() over(partition by title, type order by show_id) as rn
  from netflix_raw
)
-- Selecting desired columns and casting date_added to date, inserting into netflix table
select show_id, type, title, cast(date_added as date) as date_added, release_year,
rating, case when duration is null then rating else duration end as duration, description
into netflix
from cte;

-- Selecting all rows from netflix table
select * from netflix;

-- Splitting genres into individual rows and inserting into netflix_genre table
select show_id, trim(value) as genre
into netflix_genre
from netflix_raw
cross apply string_split(listed_in, ','); -- to split comma sep value in a cell like multiple directors of a movie

-- Selecting all rows from netflix_raw table
select * from netflix_raw;

-- New table for listed_in, director, country, cast

-- Data type conversions for date added 

-- Populate missing values in country and duration columns
insert into netflix_country
select show_id, m.country 
from netflix_raw nr
inner join (
  select director, country
  from netflix_country nc
  inner join netflix_directors nd on nc.show_id=nd.show_id
  group by director, country
) m on nr.director=m.director
where nr.country is null;

-- Selecting all rows where director is '<director name>'
select * from netflix_raw where director='<director name>';

-- Selecting director and country combinations
select director, country
from netflix_country nc
inner join netflix_directors nd on nc.show_id=nd.show_id
group by director, country;

-- Selecting all rows where duration is null
select * from netflix_raw where duration is null;

-- Populate rest of the nulls as not_available
-- Drop columns director, listed_in, country, cast


-- Netflix data analysis

/* 1. For each director, count the number of movies and TV shows created by them in separate columns 
for directors who have created both TV shows and movies */
select nd.director, 
COUNT(distinct case when n.type='Movie' then n.show_id end) as no_of_movies,
COUNT(distinct case when n.type='TV Show' then n.show_id end) as no_of_tvshow
from netflix n
inner join netflix_directors nd on n.show_id=nd.show_id
group by nd.director
having COUNT(distinct n.type)>1;

-- 2. Which country has the highest number of comedy movies
select top 1 nc.country, COUNT(distinct ng.show_id) as no_of_movies
from netflix_genre ng
inner join netflix_country nc on ng.show_id=nc.show_id
inner join netflix n on ng.show_id=nc.show_id
where ng.genre='Comedies' and n.type='Movie'
group by nc.country
order by no_of_movies desc;

-- 3. For each year (as per date added to Netflix), which director has the maximum number of movies released
with cte as (
  select nd.director, YEAR(date_added) as date_year, count(n.show_id) as no_of_movies
  from netflix n
  inner join netflix_directors nd on n.show_id=nd.show_id
  where type='Movie'
  group by nd.director, YEAR(date_added)
),
cte2 as (
  select *, ROW_NUMBER() over(partition by date_year order by no_of_movies desc, director) as rn
  from cte
)
select * from cte2 where rn=1;

-- 4. What is the average duration of movies in each genre
select ng.genre, avg(cast(REPLACE(duration,' min','') AS int)) as avg_duration
from netflix n
inner join netflix_genre ng on n.show_id=ng.show_id
where type='Movie'
group by ng.genre;

-- 5. Find the list of directors who have created both horror and comedy movies.
-- Display director names along with number of comedy and horror movies directed by them
select nd.director, 
count(distinct case when ng.genre='Comedies' then n.show_id end) as no_of_comedy, 
count(distinct case when ng.genre='Horror Movies' then n.show_id end) as no_of_horror
from netflix n
inner join netflix_genre ng on n.show_id=ng.show_id
inner join netflix_directors nd on n.show_id=nd.show_id
where type='Movie' and ng.genre in ('Comedies','Horror Movies')
group by nd.director
having COUNT(distinct ng.genre)=2;

-- Selecting genres for shows directed by Steve Brill
select * from netflix_genre where show_id in 
(select show_id from netflix_directors where director='Steve Brill')
order by genre;
