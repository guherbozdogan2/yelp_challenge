#!/bin/bash

printf "Starting Application\n"
data_dir="data"

#extracting tars 
 tar -zxvf "$1" -C "$data_dir"

demo_machine_name="smackDemoGuher3"

eval "$(docker-machine env $demo_machine_name)"

docker-machine start "$demo_machine_name"

docker-machine env "$demo_machine_name"

#docker-compose up -d