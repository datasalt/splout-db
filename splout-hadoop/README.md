Splout-Hadoop API
=================

This module allows you to seamlessly deploy data to Splout through either command-line tools or a comprehensive Hadoop Java API. To get started, make sure you have a Hadoop installation (stable 1.0.4, 0.20.X or similar) running in your computer and:

<pre>
	mvn package
	cd target
	tar xvfz splout-hadoop-0.1-SNAPSHOT-distro.tar.gz
	cd splout-hadoop-0.1-SNAPSHOT/
	hadoop jar splout-hadoop-0.1-SNAPSHOT-hadoop.jar
</pre>

If Hadoop is properly installed and HADOOP_HOME set, you will see a list of tools printed in the screen.

Command-line tools
------------------

Out of the tools included in the Hadoop driver, these are the most important ones:

- **simple-generate**: A tool for converting a CSV file into a tablespace with just one table. See <code>generate</code> tool for multiple table or multiple tablespace cases.
- **generate**: A tool for generating tablespaces from existing files (CSV).
- **deploy**: A tool for deploying tablespaces generated with tools like <code>generate</code> or <code>simple-generate</code> into an existing Splout cluster

Right now, *splout-hadoop* allows you to easily index and deploy CSV files in your HDFS / S3 / local-file-system either by **simple-generate** or **generate** followed by **deploy**.

You can now follow the [world-data example](https://github.com/datasalt/splout-db/tree/master/splout-hadoop/examples/world) to get introduced with these tools.

Java API
========

For more advanced use cases you can use the Java API. You will see an example of such usage in the [Wikipedia Page Counts Example](https://github.com/datasalt/splout-db/blob/master/splout-hadoop/src/main/java/com/splout/db/examples/PageCountsExample.java).

Data partitioning & indexing
----------------------------

The basis of Splout is partitioning datasets. Each of the partitions are then indexed using the needed structures for serving SQL queries. Partitions are determined depending on a *partitioning key* which usually is made up by one or several columns of the dataset being indexed, but this partition key can also be anything: a JavaScript method if you want - see the [TableBuilder API](https://github.com/datasalt/splout-db/blob/master/splout-hadoop/src/main/java/com/splout/db/hadoop/TableBuilder.java) for that. 

The *partitioning key* is then used in the query API so that Splout knows which node it has to ask the data 
