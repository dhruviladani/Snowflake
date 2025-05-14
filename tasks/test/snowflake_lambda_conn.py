import requests
import boto3
import pandas as pd
from datetime import datetime
import os
import io
import snowflake.connector

def lambda_handler(event, context):
    endpoints = ['products', 'carts', 'users']
    url = 'https://fakestoreapi.com/'
    s3_bucket = 'learning-dhruvi'
    s3 = boto3.client('s3')

    timestamp = datetime.now().strftime("%Y-%m-%d/%H:%M")

    for endpoint in endpoints:
        response = requests.get(url + endpoint)

        df = pd.DataFrame(response.json())

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        s3.put_object(Bucket=s3_bucket, Key=f'{endpoint}/{timestamp}.csv', Body=csv_buffer.getvalue())

        print(f"Uploaded ...")

        if endpoint == 'products':
            df['effective_start_date'] = pd.to_datetime(datetime.utcnow())
            df['is_current'] = True
            rating_df = pd.json_normalize(df['rating'])
            df['rating_rate'] = rating_df['rate']
            df['rating_count'] = rating_df['count']
            df.drop(columns=['rating'], inplace=True)

            conn = snowflake.connector.connect(
                user='EXAM',
                password='Exam@123456789',
                account='NQCWQYK-CHB73022',
                warehouse='COMPUTE_WH',
                database='EXAM',
                schema='EXAM_SCHEMA'
            )
            cs = conn.cursor()

            try:
                # Create temp staging table
                cs.execute("""
                    CREATE OR REPLACE TABLE productstable (
                        id INT,
                        title STRING,
                        price FLOAT,
                        description string,
                        category STRING,
                        image string,
                        rating_rate FLOAT,
                        rating_count INT,
                        is_current BOOLEAN
                    )
                """)

                # Write to Snowflake table
                for _, row in df.iterrows():
                    cs.execute("""
                        INSERT INTO productstable (id, title, price, description, category, image, rating_rate, rating_count, is_current)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        row['id'], row['title'], row['price'], row['description'],
                        row['category'], row['image'], row['rating_rate'], row['rating_count'],
                        row['is_current']
                    ))
                    
                    cs.execute("""
                    CREATE OR REPLACE TABLE EXAM.EXAM_SCHEMA.products_dim (
                        id INT,
                        title STRING,
                        price FLOAT,
                        description STRING,
                        category STRING,
                        image STRING,
                        rating_rate FLOAT,
                        rating_count INT,
                        is_current BOOLEAN,
                        effective_start_date TIMESTAMP,
                        effective_end_date TIMESTAMP
                        ); """)

                cs.execute("""
                    merge into products_dim as target
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
                """)

                cs.execute("""
                    merge into products_dim as target 
                    using productstable as source
                    on (target.id = source.id  and  target.is_current = TRUE)
                    when not matched then 
                        INSERT (id, title, price, description, category, image, rating_rate, rating_count,
                                is_current, effective_start_date, effective_end_date)
                                VALUES (source.id, source.title, source.price, source.description,
                                source.category, source.image, source.rating_rate, source.rating_count,
                                TRUE, CURRENT_TIMESTAMP, NULL)
                """)

                conn.commit()
                print("SCD Type 2 MERGE completed for products.")

            finally:
                cs.close()
                conn.close()
