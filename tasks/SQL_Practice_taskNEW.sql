create or replace file format csv_format_new
    type = 'csv',
    skip_header =1,
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';
 
with cte as (
    select 
        ROW_NUMBER() over (partition by 1 order by 1) as rownum,
        case when $1='Chart ID :' then $3 end as chart_id,
        case when $1='Client Name :' then $3 end as client_name,
        case when $1='Start Date :' then $3 end as start_date,
        case when $19='End Date :' then $24 end as end_date,
        case when $1 regexp '[0-9]{4}' then $1 end as task_id,
        case when $1 regexp '[0-9]{4}' then $2 end as task_name,
        case when $6 regexp '[0-9]' then $6 end as times,
        $9 as sun,
        $12 as mon,
        $16 as tue,
        $19 as wed,
        $23 as thu,
        $27 as fri,
        $30 as sat
        from @s1/Practical_data.csv
        (file_format => csv_format_new) 
), CTE1 as (
select 
    coalesce(lag(chart_id) ignore nulls over ( order by rownum),chart_id) as chart_id
    ,coalesce(client_name,lag(client_name) ignore nulls over ( order by rownum)) as client_name,
    coalesce(lag(start_date) ignore nulls over ( order by rownum),start_date) as start_date,
    coalesce(end_date,lag(end_date) ignore nulls over ( order by rownum)) as end_date,
    task_id,task_name,times,sun,mon,tue,wed,thu,fri,sat,'WPT- Willswillae' as location
    from cte 
) select * from CTE1 where TIMES is not null;
