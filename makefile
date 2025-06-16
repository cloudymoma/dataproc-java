pwd := $(shell pwd)

build:
	mvn clean package

run_serverless: 
	$(pwd)/run.sh
	
.PHONY: build run_serverless
