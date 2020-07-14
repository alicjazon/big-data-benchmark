CREATE EXTERNAL TABLE ecommerce (
    event_time timestamp,
    product_id string,
    category_id string,
    category_code string,
    category_general string,
    subcategory string,
    brand string,
    price double,
    user_id string)
PARTITIONED BY (event_type string, date_key int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '
STORED AS TEXTFILE
LOCATION 'wasbs://ecomm@mgrhadoop1hdistorage.blob.core.windows.net/ecommerce/';
LOCATION 'wasbs:///ecommerce/';
LOCATION '/ecommerce/';