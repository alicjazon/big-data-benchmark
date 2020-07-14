CREATE TABLE `ecommerce`(
`event_time` timestamp,
`event_type` string,
`product_id` string,
`category_id` string,
`category_code` string,
`brand` string,
`price` double,
`user_id` string,
`user_session` string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT
'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
'wasb://mgr-hadoop2-2020-06-03t10-26-51-320z@mgrhadoop2hdistorage.blob.core.windows.net/hive/warehouse/ecommerce'