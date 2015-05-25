#!/bin/sh -ex
mvn clean package dependency:copy-dependencies -DoutputDirectory=target/
