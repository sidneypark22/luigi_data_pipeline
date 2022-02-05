drop table if exists pres.dim_date;
CREATE table if not exists pres.dim_date (
  date_key               INTEGER                     NOT NULL PRIMARY KEY,
  -- DATE
  date             DATE                        NOT NULL,
  -- YEAR
  year_number           int                    NOT NULL,
  week_of_year_number      int                    NOT NULL,
  day_of_year_number       int                    NOT NULL,
  -- QUARTER
  quarter_number            int                    NOT NULL,
  year_quarter_number	int not null,
  -- MONTH
  month_number          int                    NOT NULL,
  month_name            VARCHAR(9)                     NOT NULL,
  month_name_short            VARCHAR(3)                     NOT NULL,
  year_month_number int not null,
  year_month_name_short VARCHAR(8) not null,
  day_of_month_number      int                    NOT NULL,
  -- WEEK
  day_of_week_number       int                    NOT NULL,
  -- DAY
  day_name              VARCHAR(9)                     NOT NULL,
  day_name_short              VARCHAR(3)                     NOT NULL,
  is_last_day_of_month  VARCHAR(1)                    NOT null,
  etl_created_datetime datetime,
  etl_updated_datetime datetime,
  unique (date)
) DISTSTYLE ALL SORTKEY (date_key);


insert into pres.dim_date
with recursive
      start_dt as (select cast('2010-01-01' as date) s_dt)
      , end_dt as (select cast('2030-01-01' as date) e_dt)
      -- the recusive cte, note declaration of the column `dt`
      , dates (dt) as (
            -- start at the start date
            select s_dt dt from start_dt
            union all
            -- recursive lines
            select dateadd(day, 1, dt)::date dt  -- converted to date to avoid type mismatch
            from dates
            where dt <= (select e_dt from end_dt)  -- stop at the end date
      )
select cast(to_char(dt, 'yyyymmdd') as int) as date_key
, dt as date
, date_part('year', dt) as year_number
, date_part('week', dt) as week_of_year_number
, date_part('dy', dt) as day_of_year_number
, date_part('qtr', dt) as quarter_number
, cast(cast(date_part('year', dt) as varchar) + '0' +  right(cast(date_part('qtr', dt) as varchar), 2) as int) year_quarter_number
, date_part('month', dt)  as month_number
, INITCAP(to_char(dt, 'month')) as month_name
, INITCAP(to_char(dt, 'mon')) as month_name_short
, cast(cast(date_part('year', dt) as varchar) + '0' +  right(cast(date_part('month', dt) as varchar), 2) as int) as year_month_number
, cast(date_part('year', dt) as varchar) + '-' + INITCAP(to_char(dt, 'mon')) as year_month_name_short
, date_part('day', dt) as day_of_month_number
, cast(replace(date_part('dow', dt), 0, 7) as int) as day_of_week_number
, INITCAP(to_char(dt, 'day')) as day_name
, left(INITCAP(to_char(dt, 'day')), 3) as day_name_short
, case when date_part('month', dt) <> date_part('month', dt + 1) then 'Y' else 'N' end as is_last_day_of_month
, date_trunc('second', cast(current_timestamp as datetime)) as etl_created_datetime
, date_trunc('second', cast(current_timestamp as datetime)) as etl_updated_datetime
from dates
--limit 100

