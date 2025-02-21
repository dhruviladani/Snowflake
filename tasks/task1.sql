create database business_task;

select $1 from @Task_stage;

create or replace file format csv_format 
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1;


create table BUSINESS(
    Business varchar(20),
    LOCATION varchar(20),
    EASTERN_TIMEZONE_CONVERSION varchar(20),
    visitid number(5),
    client varchar(20),
    Time_Of_Visit time
);

copy into BUSINESS_TASK.PUBLIC.BUSINESS 
from @TASK_STAGE/BusinessData_task2.csv
FILE_FORMAT = (FORMAT_NAME = 'csv_format');

select * from BUSINESS_TASK.PUBLIC.BUSINESS ;


ALTER TABLE BUSINESS_TASK.PUBLIC.BUSINESS  ADD time_interval varchar(20) NULL;

ALTER TABLE BUSINESS_TASK.PUBLIC.BUSINESS
DROP time_interval;

UPDATE BUSINESS_TASK.PUBLIC.BUSINESS
SET time_of_visit = '16:00:00'
WHERE BUSINESS = 'Metro Logistics';


update BUSINESS_TASK.PUBLIC.BUSINESS 
set time_interval = case 
    when minute(time_of_visit) < 30 then 
        concat(HOUR(time_of_visit), ':00 - ', HOUR(time_of_visit), ':30')
    else 
        concat(HOUR(time_of_visit), ':30 - ', HOUR(time_of_visit) + 1, ':00')
end;

select * from BUSINESS_TASK.PUBLIC.BUSINESS ;

insert into BUSINESS_TASK.PUBLIC.BUSINESS values(
    'OceanBreezeResorts',
    'MIAMI EAST',
    '10:00-18:00',
    357,
    'd',
    '11:45:00',
    null
)
