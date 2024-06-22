CREATE EXTERNAL TABLE `accelerometer_trusted`(
  `z` double COMMENT 'from deserializer', 
  `user` string COMMENT 'from deserializer', 
  `y` double COMMENT 'from deserializer', 
  `x` double COMMENT 'from deserializer', 
  `timestamp` bigint COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://dragos-stedi/accelerometer/trusted/'
TBLPROPERTIES (
  'CreatedByJob'='Accelerometer Landing to Trusted', 
  'CreatedByJobRun'='jr_4ffcd63a54da9c46d858f3e053af5f26ee2ae1e0ba96660c385b66daee024d65', 
  'classification'='json')
