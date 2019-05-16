#!/bin/bash

printf "Setting up Application\n"
data_dir="data"

#extracting tars
printf "Extracting Tar File\n"
 tar -zxvf "$1" -C "$data_dir"


printf "Creating Virtual Machine \n"

demo_machine_name="guhersDemoMachine"

docker-machine create -d virtualbox --virtualbox-memory "7000" --virtualbox-cpu-count "4" "$demo_machine_name"

eval "$(docker-machine env $demo_machine_name)"

docker-machine start "$demo_machine_name"

docker-machine env "$demo_machine_name"

printf "Running containers \n"
docker-compose up


