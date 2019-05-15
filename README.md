## Prerequisites

Docker is necessary for building.
- Docker Version: 18.09.2



## Setting Up Environment and running
### Creating docker machines and running containers
To establish the execution of spark tasks, the following script should be executed with providing the path of the data file:
```
	  chmod +x *.sh
	  ./run.sh TAR_GZ_FILE_LOCATION
```

### Testing db tables
To test the db, the cqlsh client of cassandra container should be executed as:
```
	>docker exec -ti  yelp_challenge_cassandra_1 bash
	>cqslh
	>describe test.tables
	>select * from test.photo limit 10
	
```

## The container image built for solution
The generated image used to create the task named service in compose.yaml file is taken from following repository:
	docker pull guherbozdogan2/repo
	
The generation has been established via sbt docker plugin(via sbt sbt docker:publishLocal command inside this project folder). The command generates Docker file and it's necessary lib/bin folders (with populated applications).

The spark application is ran lke a standalone spark application. The best condition would be creating a new Docker container for Spark Executors (Docker using Mesos' project's spark executors) and using spark-submit. This solution currently uses a stand alone spark application instead of utilizing spark-submit( spark client/cluster mode). 


