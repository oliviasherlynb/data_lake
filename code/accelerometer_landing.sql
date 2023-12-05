CREATE EXTERNAL TABLE IF NOT EXISTS `stedi-database`.`accelerometer_landing` (
  `user` string,
  `timestamp` timestamp,
  `x` float,
  `y` float,
  `z` float
)
  
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
  
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://os-stedi-lakehouse/accelerometer/landing/'
TBLPROPERTIES ('classification' = 'json');
