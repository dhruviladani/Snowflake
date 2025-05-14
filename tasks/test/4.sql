CREATE or replace TABLE products (
    product_id INT,
    title STRING,
    price FLOAT,
    description string,
    category STRING,
    image string,
    rating string
);

CREATE TABLE carts (
    cart_id INT,
    user_id INT,
    date TIMESTAMP,
    products VARIANT,
    last_updated_at TIMESTAMP
);

CREATE TABLE users (
    user_id INT,
    email STRING,
    username STRING,
    password STRING,
    address OBJECT,
    phone STRING
);





CREATE OR REPLACE STORAGE INTEGRATION s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::507325772253:role/s3bucketaccessrole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://learning-dhruvi');

DESC INTEGRATION s3_integration;


CREATE OR REPLACE STAGE s3_stage
URL='s3://learning-dhruvi/*'
storage_integration = s3_integration
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

list @s3_stage;
LIST @s3_stage/products/2025-05-12/;
LIST @s3_stage/carts/2025-05-12/;
LIST @s3_stage/users/2025-05-12/;
----------------------------

CREATE OR REPLACE FILE FORMAT csvformat1
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1;



COPY INTO products
FROM @directaccess/products/2025-05-12/16:30.csv
FILE_FORMAT = (FORMAT_NAME = 'csvformat1');

select * from products;




-----------------------------

select * from products_dim;









---------------------------------

create or replace procedure proc ()
returns varchar 
language sql
as 
$$
begin 
    merge into EXAM.EXAM_SCHEMA.PRODUCTS_DIM as target
    using productstable as source 
    on (target.id = source.id  and  target.is_current = TRUE) 
    when matched and (  target.title <> source.title OR
                        target.price <> source.price OR
                        target.description <> source.description OR
                        target.category <> source.category OR
                        target.image <> source.image OR
                        target.rating_rate <> source.rating_rate OR
                        target.rating_count <> source.rating_count )
    
    then 
        update set target.is_current = FALSE, target.effective_end_date = CURRENT_TIMESTAMP;


    merge into EXAM.EXAM_SCHEMA.PRODUCTS_DIM as target 
    using productstable as source
    on (target.id = source.id  and  target.is_current = TRUE)
    when not matched then 
        INSERT (id, title, price, description, category, image, rating_rate, rating_count,
                            is_current, effective_start_date, effective_end_date)
                            VALUES (source.id, source.title, source.price, source.description,
                            source.category, source.image, source.rating_rate, source.rating_count,
                            TRUE, CURRENT_TIMESTAMP, NULL);

    return ;
                            
END;
$$;

call proc(); 
