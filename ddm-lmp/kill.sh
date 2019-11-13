#!/usr/bin/env bash

RED='\033[0;31m'
NC='\033[0m' # No Color
WAIT_SECONDS=10

echo ${RED}Kill all java processes${NC}
killall java
