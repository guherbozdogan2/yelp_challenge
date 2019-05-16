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

## The container image's build steps (not necessary to do, since its already ready in docker hub)
The generated image used to create the "task" named container process (task service docker-compose,yml) is taken from following repository:
	docker pull guherbozdogan2/repo:latest

The generation has been established via this scala-sbt project via sbt docker plugin(via sbt sbt docker:publishLocal command inside this project folder). The command generates Docker file and it's necessary lib/bin folders (with populated applications). Accordingly the above docker hub repo has been generated from the repo with the following steps: (The below steps are not necessary to do for to run project.  Only ./run.sh command execution is enough to run project as since the docker image is alreadily existing in docker hub repo: guherbozdogan2/repo:latest )

```
    >sbt update clean compile package
    >sbt docker:publishLocal
    >cd docker/target/stage
```

    Change the line of: 'ENTRYPOINT ["/opt/docker/bin/yelp-conversion-task"]' to 'ENTRYPOINT /opt/docker/bin/main-task && /opt/docker/bin/main-test-task' and build local image with:
```
    >docker build -t repo .
```

## Current Limitations to be fixed
- The spark application is ran lke a standalone spark application. The best ran condition would be creating a new Docker image based on Spark images for Spark Executors (Docker using Mesos' project's spark executors) and using spark-submit instead of standalone application. This solution currently uses a stand alone spark application instead of utilizing spark-submit( spark client/cluster mode).
- Unit tests are missing
- Integration test cases have low coverage and should be automated wth a test framework
- CI tool integration for builds/tests + log server integration for spark logs/docker compose logs analysis


## Queries in integration tests
In integration tests, the following 2 cases are tested for each table: (This task executes right after the migration completes and the tests results are written with Success/Error logs output in console currently(With having prefix of "*******" )

- The equivalence of cardinality of RDD data and Cassandra table based on partition key  (grouped by partition key)
- Whether sampled 1000 different records (With different partititon keys from RDD) also exist in cassandra table


Sample outputs in console after executing ./run.sh: 

```
******************Testing whether cardinality of RDD is same as CQL Table's cardinality in table:business
******************Success in:Testing whether cardinality of RDD is same as CQL Table's cardinality in table:business
```

```
******************Testing randomly sampled 1000 records existence in table:business
******************Success in: Testing randomly sampled 1000 records existence in table:business
```
