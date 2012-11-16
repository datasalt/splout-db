Splout SQL Server
=================

The Splout SQL Server implements the REST API for being able to serve SQL queries and deploy or rollback an arbitrary number of *tablespaces*. The server is made up by two services: the *DNode* and the *QNode*. The *DNode* is in charge of communicating directly with the database files whereas the *QNode* implements the REST API and delegates queries to the appropriate *DNodes*.

Getting started
---------------

For starting playing with *splout-server*:

<pre>
  mvn package
  cd target
  tar xvfz splout-server-0.1-SNAPSHOT-distro.tar.gz
  cd splout-server-0.1-SNAPSHOT/
  bin/splout-service.sh qnode start
  bin/splout-service.sh dnode start
</pre>

Now, if everything went fine, and assuming you had nothing running on port 4412 before, you will see a nice panel in localhost:4412 like this:

![panel](https://raw.github.com/datasalt/splout-db/master/splout-server/Panel.png)

In logs/ folder you will find the logs of both the *QNode* and the *DNode* service. You can shutdown the services with:

<pre>
  bin/splout-service.sh qnode stop
  bin/splout-service.sh dnode stop
</pre>

Populating the server
---------------------

Splout is *read-only* and populates its data from Hadoop-generated datasets. If you want to populate and play with example datasets go to [splout-hadoop](https://github.com/datasalt/splout-db/tree/master/splout-hadoop) and continue from there. 

Server configuration
--------------------

For configuring and fine tuning a Splout server, you can take a look to the [default configuration file](https://github.com/datasalt/splout-db/blob/master/splout-server/src/main/resources/splout.properties.default). This file is bundled with the server JAR and loaded as a resource. If you want to override or enable some property, you need to create a **new file** called **splout.properties**. Because Splout uses a double-configuration mechanism, defaults are never modified and instead you specify which properties you enable or override in this new file, which should be accessible either as a resource in your classpath or just be in the same folder where you launch the services.

Distributed configuration
-------------------------

Splout uses [Hazelcast](http://www.hazelcast.com/) for coordinating a distributed cluster. You can launch a distributed cluster in three different ways:

- Using **MULTICAST** (default). For that, default configuration is enough.
- Using **Amazon EC2** <code>hz.join.method=aws</code>. For that, you need to specify your credentials with <code>hz.aws.key</code> and <code>hz.aws.secret</code> and optionally an Amazon security group with <code>hz.aws.security.group</code> (otherwise all alive EC2 instances are examined).
- Using fixed **TCP/IP** addresses <code>hz.join.method=tcp</code>. For that, you need to specify the comma-separated list of host:port with <code>hz.tcp.cluster</code>. You can control the port that each service will bind to with <code>hz.port</code>.

REST API overview
-----------------

You can interact with the server through the [Java client](https://github.com/datasalt/splout-db/tree/master/splout-javaclient) or directly through the REST interface. These are the basic methods of the REST interface:

* GET, *api/overview*

* GET, *api/dnodelist*

* GET, *api/query/tablespace?key=theKey&sql=sqlQuery*

* POST, *api/deploy* (body is a list of [DeployRequest](https://github.com/datasalt/splout-db/blob/master/splout-commons/src/main/java/com/splout/db/qnode/beans/DeployRequest.java))
* POST, *api/rollback* (body is a list of [SwitchVersionRequest](https://github.com/datasalt/splout-db/blob/master/splout-commons/src/main/java/com/splout/db/qnode/beans/SwitchVersionRequest.java))
