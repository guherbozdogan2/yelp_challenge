 #!/bin/bash
docker exec -it yelp_challenge_cassandra_1   cqlsh -f  data/db/migration/migrate.cql