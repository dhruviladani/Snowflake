create or replace file format csv_format
    type = 'csv'
    skip_header = 1
    field_delimiter = ','
    -- field_optionally_enclosed_by = '"'
    EMPTY_FIELD_AS_NULL = TRUE
    TRIM_SPACE = TRUE;

select $1, replace($2, '"',''), $3 ,
case 
    WHEN $5 LIKE '%APT #%' THEN CONCAT('APT', ' ', SPLIT_PART($5, 'APT #', 2))
    WHEN $5 LIKE '%#%' THEN SPLIT_PART($5, '#', 2)
        WHEN $5 LIKE '%APT%' THEN CONCAT('APT', ' ', TRIM(SPLIT_PART($5, 'APT', 2)))
        WHEN $5 LIKE '%AOT%' THEN SPLIT_PART($5, 'AOT', 2)
        WHEN $5 LIKE '%UNIT%' THEN SPLIT_PART($5, 'UNIT', 2)
        ELSE '-'
    end as apt_no,

CASE 
        WHEN $5 LIKE '%#%' THEN TRIM(SPLIT_PART($5, '#', 1))
        WHEN $5 LIKE '%APT%' THEN TRIM(SPLIT_PART($5, 'APT', 1))
        WHEN $5 LIKE '%AOT%' THEN TRIM(SPLIT_PART($5, 'AOT', 1))
        WHEN $5 LIKE '%UNIT%' THEN Trim(SPLIT_PART($5, 'UNIT', 1))
        ELSE $5
    END AS Address_1
from @EXAM.EXAM_SCHEMA.CLEANING/DATA-CLEANING.csv (file_format => csv_format) ;

-- create or replace table clean (
--     id string,
--     name string,
--     address string
-- );

-- copy into EXAM.EXAM_SCHEMA.CLEAN from @EXAM.EXAM_SCHEMA.CLEANING/DATA-CLEANING.csv
-- file_format = (format_name = 'csv_format');

-- select * from EXAM.EXAM_SCHEMA.CLEAN;

-- update table clean name ;

-- SELECT * FROM clean, LATERAL FLATTEN(input=>split(name, ','));


select split(name , ',') from clean;


select $1,
replace($2,'"','') as first_name,
$3 as last_name,
CASE 
        WHEN $5 LIKE '%#%' THEN TRIM(SPLIT_PART($5, '#', 2))
        WHEN $5 LIKE '%APT%' THEN TRIM(SPLIT_PART($5, 'APT', 2))
        WHEN $5 LIKE '%AOT%' THEN TRIM(SPLIT_PART($5, 'AOT', 2))
        ELSE NULL
    END AS Apartment_number,
    CASE 
        WHEN $5 LIKE '%#%' THEN TRIM(SPLIT_PART($5, '#', 1))
        WHEN $5 LIKE '%APT%' THEN TRIM(SPLIT_PART($5, 'APT', 1))
        WHEN $5 LIKE '%AOT%' THEN TRIM(SPLIT_PART($5, 'AOT', 1))
        ELSE $5
    END AS Address_1
from @EXAM.EXAM_SCHEMA.CLEANING/DATA-CLEANING.csv
(file_format => csv_format);
