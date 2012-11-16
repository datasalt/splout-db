Splout SQL: A web-latency SQL spout for Hadoop.
===============================================

Splout is a scalable, open-source, easy-to-manage SQL big data view. Splout is to Hadoop + SQL what Voldemort or Elephant DB are to Hadoop + Key/Value. Splout serves a read-only, partitioned SQL view which is generated and indexed by Hadoop. In a nutshell:

- Splout allows serving an **arbitrarily big dataset** with **high QPS rates** and at the same time provides **full SQL** query syntax.
- Splout uses Hadoop for **decoupling database creation** from database serving. The data is always **optimally indexed** and evenly distributed among partitions, plus updating all the data at once doesn’t affect the serving of the datasets.
- Splout provides an **easy-to-manage API** for deploying partitioned datasets. Splout manages many *tablespaces* and their versioning, allowing for **rollback of** previous versions when needed.
- Splout is **horizontally scalable**. You can increase throughput linearly by just adding more machines.
- Splout is **replicated for failover**. Splout transparently replicates your dataset for properly handling failover scenarios such as network splits or hardware corruption.
- Splout is very **flexible**. Even though it is relational, by being a read-only store with atomic data loading we allow the user to change the entire data model from one day to another, seamlessly, without pain.
- Splout **plays well with Hadoop**. Splout uses [Pangool](https://pangool.net) for balancing, indexing and deploying your data seamlessly without affecting the serving of queries through friendly command-line tools and a more sofisticated Java API. 
- Splout is **ready for the web**. Splout is appropriated for web page serving, many low latency lookups, scenarios such as mobile or web applications with demanding performance. Splout offers a *REST* API that returns *JSON* for any query.

Architecture
------------

The overall architecture is quite simple:

- [splout-server](https://github.com/datasalt/splout-db/tree/master/splout-server) is installed on a cluster of commodity hardware machines. Each of the machines runs a DNode service and optionally a QNode service.
- QNodes implement a REST API for serving users' queries.
- QNodes talk to the appropriate DNode for serving a query and the DNode responds back with the query’s result.
- A Hadoop cluster is used by [splout-hadoop](https://github.com/datasalt/splout-db/tree/master/splout-hadoop) for indexing and balancing the data. The resultant SQL files are fetched by the DNodes by handling a data deploy request.

![Splout arch](https://raw.github.com/datasalt/splout-db/master/Splout_SQL.jpg)

Modules
-------

Splout is divided in several modules:

- [splout-server](https://github.com/datasalt/splout-db/tree/master/splout-server): The server itself. You can use this module for running a server or a cluster of servers.
- [splout-hadoop](https://github.com/datasalt/splout-db/tree/master/splout-hadoop): Command-line tools and Java API for balancing, indexing and deploying datasets using Hadoop. You can use this module to interact with the server. It also contains examples to play with.
- *splout-javaclient*: A Java client for the *splout-server* REST API. Used by both *splout-server* and *splout-hadoop*. You can use this module in your own Java project for communicating with Splout.

The following two are only transitively needed for development and you don't need to interact with them directly:

- *splout-resources*: Contains native libraries, used by most of the modules.
- *splout-commons*: Used as a dependency by most of the modules.

Getting started
---------------

For playing with Splout you will need:
- Java 1.6+.
- [Hadoop](http://hadoop.apache.org/releases.html#Download) stable (1.0.4) or previous versions of it (0.20.X, etc) installed on your machine.

The best thing for getting started with Splout is launching a local server in your machine. You will find instructions on that in [splout-server](https://github.com/datasalt/splout-db/tree/master/splout-server). After that, you can execute one or two examples from [splout-hadoop](https://github.com/datasalt/splout-db/tree/master/splout-hadoop).