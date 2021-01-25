Splout SQL: A web-latency SQL spout for Hadoop.
===============================================

Splout is a scalable, open-source, easy-to-manage SQL big data view. Splout is to Hadoop + SQL what Voldemort or Elephant DB are to Hadoop + Key/Value. Splout serves a read-only, partitioned SQL view which is generated and indexed by Hadoop. In a nutshell:

For up-to-date documentation and examples please visit [the official website](http://datasalt.github.io/splout-db/)

Quick steps to compile it:

1) Compile it adapting the different pom.xml to your Hadoop distro versions. Use "mr2" profile for MR2 versions.

mvn clean install -DskipTests

The default version compiles the distribution for MR2 versions (MapReduce 2), the most common use case. Other
available maven profiles are:

* mr1: MarReduce 1 version
* cdh5: Cloudera CDH5 version

For example, for compiling for CDH5 version you have to launch:

mvn clean install -DskipTests -Pcdh5

If you need to compile against a particular Hadoop version, you'll need to modify the different pom.xml
acordingly. You can use the cdh5 profile as reference. 

2) Once compiled you'll find the distribution binaries are at file assembly/target/splout-distribution-*.tar.gz 
ready for being installed.
