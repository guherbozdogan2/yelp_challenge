 #!/bin/bash
docker exec -it yelp_challenge_spark_1     spark-shell  --master yarn  --deploy-mode client --executor-cores 1  --num-executors 2 --jars /target/  --conf spark.cassandra.connection.host=cassandra   --class app.task.ProcessBusinessInfo

	