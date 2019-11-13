#!/usr/bin/env bash

RED='\033[0;31m'
NC='\033[0m' # No Color
WAIT_SECONDS=10

cd target || echo "Are you sure you are in the right directory: $(pwd)"

echo -e "${RED}Starting master now${NC}"
java -jar ddm-lmp-1.0.jar master -h 127.0.0.5  &

echo -e "${RED}Sleeping for ${WAIT_SECONDS} seconds${NC}"
sleep ${WAIT_SECONDS}

echo -e "${RED}Starting slave now${NC}"
java -jar ddm-lmp-1.0.jar slave -mh 127.0.0.5 &
