#!/bin/bash
mvn clean install
scp ./target/performancetest-1.0-SNAPSHOT.jar devuser@hordor26-n1.cs1cloud.internal:/home/devuser/dmoore/performance_testing/
