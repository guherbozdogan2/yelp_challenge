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

## The image used in solution
The generated image used to create the task named service in compose.yaml file is taken from following repository:
	docker pull guherbozdogan2/repo
	
The generation has been established in scala project via sbt docker plugin(via sbt sbt docker:publishLocal command). The command generates Docker file for above repo and it's lib/bin folders (with populated applications).
