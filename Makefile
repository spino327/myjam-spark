SHELL=/usr/bin/env sh

# PATH to spark-submit
SPARK_SUBMIT=spark-submit

# DEBUG
DEBUG="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8000 -Xnoagent -Djava.compiler=NONE"

# Default rule
.PHONY: help
help:
	@echo "MyJam helper scripts"
	@echo "Available rules:"
	@echo "  download"
	@echo "    Downloads and extracts the thisismyjam dataset"
	@echo "  dist"
	@echo "    Build and creates the fat jar for the project."
	@echo "  debug"
	@echo "    Builds and runs in remote debug mode the project. Port 8000"
	@echo "  test_debug"
	@echo "    Builds and runs in remote debug mode the tests. Port 8000"
	@echo "  clean"
	@echo "    Cleans the project with 'mvn clean'"
	@echo "  eclipse"
	@echo "    Setups eclipse's project"

.PHONY: download
download:
	@echo "Dowloading the dataset..."
	-wget -c https://archive.org/download/thisismyjam-datadump/thisismyjam-datadump.zip
	@echo "Extracting..."
	- unzip thisismyjam-datadump.zip

.PHONY: dist
dist:
	- mvn package

.PHONY: debug
debug:
	- mvn compile
	- rm -r output
	- $(SPARK_SUBMIT) --conf 'spark.executor.extraJavaOptions=$(DEBUG)' --conf 'spark.driver.extraJavaOptions=$(DEBUG)' --master local[4] --class com.spotify.jam.MyJamApp target/classes archive/jams.tsv archive/likes.tsv archive/followers.tsv ./output 4

.PHONY: test_debug
test_debug:
	- mvn -Dmaven.surefire.debug=$(DEBUG) test

.PHONY: clean
clear:
	- mvn clean

.PHONY: eclipse
eclipse:
	- mvn eclipse:eclipse
