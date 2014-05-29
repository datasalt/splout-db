[![Build Status][1]][2]

Splout SQL: A web-latency SQL spout for Hadoop.
===============================================

Splout is a scalable, open-source, easy-to-manage SQL big data view. Splout is to Hadoop + SQL what Voldemort or Elephant DB are to Hadoop + Key/Value. Splout serves a read-only, partitioned SQL view which is generated and indexed by Hadoop. In a nutshell:

For up-to-date documentation and examples please visit [the official website](http://sploutsql.com)

Quick steps to use it:

1) Compile it adapting the different pom.xml to your Hadoop distro versions. Use "mr2" profile for MR2 versions.

mvn clean install -DskipTests -Pmr2

2) You should use the 
file assembly/target/splout-distribution-0.2.6-SNAPSHOT-mr2.tar.gz 

[1]: http://clinker.datasalt.com/desktop/plugin/public/status/splout-build
[2]: http://clinker.datasalt.com/jenkins/view/SploutSQL
