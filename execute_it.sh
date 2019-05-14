 #!/bin/bash


docker exec -d -e YARN_CONF_DIR="spark/conf"   yelp_challenge_spark_1 spark/bin/spark-shell     spark-shell  --master yarn  --deploy-mode client --executor-cores 1  --num-executors 2 --jars "/target/scala-2.11//Yelp Conversion-assembly-1.0.jar"  --conf spark.cassandra.connection.host=cassandra   --class app.task.ProcessBusinessInfo

docker exec -d -e YARN_CONF_DIR="spark/conf"   yelp_challenge_spark_1 spark/bin/spark-shell     spark-shell  --master yarn  --deploy-mode client --executor-cores 1  --num-executors 2 --jars "/target/scala-2.11//Yelp Conversion-assembly-1.0.jar"  --conf spark.cassandra.connection.host=cassandra   --class app.task.ProcessBusinessInfo

docker exec -d -e YARN_CONF_DIR="spark/conf"   yelp_challenge_spark_1 spark/bin/spark-shell     spark-shell  --master yarn  --deploy-mode client --executor-cores 1  --num-executors 2 --jars "/target/scala-2.11//Yelp Conversion-assembly-1.0.jar"  --conf spark.cassandra.connection.host=cassandra   --class app.task.ProcessBusinessInfo

docker exec -d -e YARN_CONF_DIR="spark/conf"   yelp_challenge_spark_1 spark/bin/spark-shell     spark-shell  --master yarn  --deploy-mode client --executor-cores 1  --num-executors 2 --jars "/target/scala-2.11//Yelp Conversion-assembly-1.0.jar"  --conf spark.cassandra.connection.host=cassandra   --class app.task.ProcessBusinessInfo

	