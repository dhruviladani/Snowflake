create or replace file format json_format
    type = 'json'
    strip_outer_array = true;


select * from @EXAM.EXAM_SCHEMA.MY_INT_STAGE
(file_format => 'json_format');

-- create or replace  table Content_data (
--     id string,
--     type string,
--     name string,
--     ppu float,
--     batters string,
--     topping string
-- );

create or replace table Content_data(
    json_data variant 

);

copy into CONTENT_DATA from @EXAM.EXAM_SCHEMA.MY_INT_STAGE/Exam-Json-Data.json
file_format = (format_name = 'JSON_FORMAT');


select json_data:id as id, json_data:type, json_data:name, json_data:ppu, batter.value:id as batter_id , batter.value:type as batter_type , topping.value:id as topping_id , topping.value:type as topping_type 
from EXAM.EXAM_SCHEMA.CONTENT_DATA, LATERAL flatten (input => json_data:batters:batter) as batter , lateral flatten(input => json_data:topping) as topping;


SELECT json_data:id AS id, json_data:type, json_data:name, json_data:ppu, batter.value AS batter   
FROM EXAM.EXAM_SCHEMA.CONTENT_DATA, LATERAL FLATTEN(input => json_data:batters:batter) AS batter;

