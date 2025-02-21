create or replace file format csv_format
    type = 'CSV'
    field_delimiter = ','
    record_delimiter = 'Chart ID :'
    skip_header = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';GARDEN_PLANTS.INFORMATION_SCHEMA

-- create or replace file format csv_format2
--     type

CREATE OR REPLACE FILE FORMAT csv_format2
    TYPE = 'CSV'
    -- FIELD_DELIMITER = ','
    -- record_delimiter = 'wk'
    SKIP_HEADER = 0 ,
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';



select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10 from @s1;

select $1,$2,$3,$37, $39, $75, $96, $325 as Taskid from @s1/Practical_data.csv (FILE_FORMAT =>  csv_format );

select $1 as TaskID, $2 as TaskName , $3 from @s1/Practical_data.csv (FILE_FORMAT =>  csv_format2) where $1 regexp '^01[0-9]{2}$' or $3 regexp 'PAT.*';


select  $3 as ChartID, $3 as client_name , $24 as end_date , $1 as TaskID, $2 as TaskName, $9 as Sun, $12 as Mon, $16 as Tues, $19 as Wed, $23 as Thur, $27 as Fri, $30 as Sat from @s1/Practical_data.csv (FILE_FORMAT =>  csv_format2) where ($12 is not null and $12 = 'X') or $1 regexp '^01[0-9]{2}$' or $3 regexp 'PAT.*' or $24 regexp '^(0?[1-9]|[12][0-9]|3[01])[\/\-](0?[1-9]|1[012])[\/\-]\d{4}$';


select  $3 as ChartID, $3 as client_name , $24 as end_date , $1 as TaskID, $2 as TaskName, $9 as Sun, $12 as Mon, $16 as Tues, $19 as Wed, $23 as Thur, $27 as Fri, $30 as Sat from @s1/Practical_data.csv (FILE_FORMAT =>  csv_format2) where ($2 is not null and $2 != 'Task') or $1 regexp '^01[0-9]{2}$' or chartid regexp 'PAT.*' or client_name regexp '^*,*$' or $24 regexp '^(0?[1-9]|[12][0-9]|3[01])[\/\-](0?[1-9]|1[012])[\/\-]\d{4}$';


select case when $1 = 'Chart ID :' then $3 end as chartid,  case when $1 = 'Client Name :' then $3 end as client_name , $1 as TaskID, $2 as TaskName, $9 as Sun, $12 as Mon, $16 as Tues, $19 as Wed, $23 as Thur, $27 as Fri, $30 as Sat from @s1/Practical_data.csv (FILE_FORMAT =>  csv_format2) where ($2 is not null and $2 != 'Task') or $1 = 'Chart ID :' or $1 = 'Client Name :';

select row_number() over order by 1,  case when $1 = 'Chart ID :' then $3 end as chartid, lag(chartid) ignore nulls over(order by rownum) , case when $1 = 'Client Name :' then $3 end as client_name , $1 as TaskID, $2 as TaskName, $9 as Sun, $12 as Mon, $16 as Tues, $19 as Wed, $23 as Thur, $27 as Fri, $30 as Sat from @s1/Practical_data.csv (FILE_FORMAT =>  csv_format2) where ($2 is not null and $2 != 'Task') or $1 = 'Chart ID :' or $1 = 'Client Name :';



with cte1 as (
    select $3 as ChartID, $39, $75, $96, $325 as Taskid from @s1/Practical_data.csv (FILE_FORMAT =>  csv_format )
), cte2 as (
    select $1 as TaskID, $2 as TaskName ,$3  as chartid from @s1/Practical_data.csv (FILE_FORMAT =>  csv_format2) where $1 regexp '^01[0-9]{2}$' or $3 regexp 'PAT.*' 
) select  * from cte1 join cte2 on cte1.chartid != cte2.chartid order by cte1.Chartid;
