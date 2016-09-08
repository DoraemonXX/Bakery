#How to run sql example

`/usr/hdp/2.4.0.0-169/spark//bin/spark-submit --class com.bakery.woople.sql.SparkSqlGuide --master yarn-client 
--deploy-mode client 
--executor-memory 1g 
--num-executors 1 
--queue default /opt/bakery-0.0.1.jar`