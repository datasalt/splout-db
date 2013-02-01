This example shows how to load data into Splout SQL and then shows an example app using the uploaded data. 

Data
=====
The data is the example data from "Daily Global Weather Measurements, 1929-2009 (NCDC,
GSOD)" http://aws.amazon.com/datasets/2759

**This data set can only be used within the United States. If you redistribute 
any of these data to others, you must include this same notification.**

The full dataset is almost 20 Gb, but we use an small sample for this example.

The data is composed by tree tables:

* meteo -> meteo.txt file. Contains the measurements per each station.
* stations -> ish-history.csv file. Contains the stations list.
* countries -> country-list.txt. Contains the list of countries.

Generate and deploy meteo table by command line
=====================================
We will see first, an example on how to use the command line to deploy sigle table tablespaces.

First of all, you should start a local Splout SQL server as shown in [splout-server](https://github.com/datasalt/splout-db/tree/master/splout-server)

Then upload the examples to the HDFS:
<pre>
	hadoop fs -put examples examples
</pre>

A tablespace containing only the meteo table can be generated with the following command:

<pre>
hadoop jar splout-*-hadoop.jar simple-generate --input examples/meteo/meteo.txt --output database-files --tablespace meteo_table --table meteo --schema "stn:int,wban:int,year:int,month:int,day:int,date:int,temp:double,temp_observations:int,dewp:double,dewp_observations:int,slp:double,slp_observations:int,stp:double,stp_observations:int,visib:double,visib_observations:int,wdsp:double,wdsp_observations:int,mxspd:double,gust:double,max:double,max_flag:string,min:double,min_flag:string,prcp:double,prcp_flag:string,sndp:double,frshtt:int" --partitionby stn,wban --partitions 4 --skipheading --fixedwidthfields "0,5,7,11,14,17,18,19,20,21,14,21,24,29,31,32,35,40,42,43,46,51,53,54,57,62,64,65,68,72,74,75,78,82,84,85,88,92,95,99,102,107,108,108,110,115,116,116,118,122,123,123,125,129,132,137" 
</pre>

And can be deployed in the Splout SQL cloud using the following command:

<pre>
hadoop jar splout-*-hadoop.jar deploy --root database-files --tablespaces meteo_table --replication 2 --qnode http://localhost:4412
</pre>

Generating Meteo from definition files
===========================

We will create two tablespaces:

Tablespace meteo-pby-stn-wban
------------------------------

The tables stations and countries is replicated in every partition meanwhile the table meteo is partitioned by **stn** and **wban** columns. The tablespace definition file can be seen here: https://github.com/datasalt/splout-db/blob/master/splout-hadoop/examples/meteo/meteo-pby-stn-wban.json

Tablespace meteo-pby-date
-------------------------

The tables stations and countries is replicated in every partition meanwhile the table meteo is partitioned by the **date** column. The tablespace definition file can be seen here: https://github.com/datasalt/splout-db/blob/master/splout-hadoop/examples/meteo/meteo-pby-date.json

The command for generate both tablespaces is the following:

<pre>
hadoop jar splout-*-hadoop.jar generate -tf examples/meteo/meteo-pby-stn-wban.json -tf examples/meteo/meteo-pby-date.json -o database-files
</pre>

Now both tablespaces are ready to be deployed atomically into the Splout SQL cluster:

<pre>
hadoop jar splout-*-hadoop.jar deploy --qnode http://localhost:4412 --root database-files --tablespaces meteo-pby-stn-wban
</pre>

If you visit http://localhost:4412/ you will be able to see the deployed tablespaces, and you will be able to perform queries at http://localhost:4412/console.html

Now let's see how an example aplication makes use of this data. Open the file examples/meteo/web/index.html in your browser and try the example. The code of the example can be seen here: https://github.com/datasalt/splout-db/blob/master/splout-hadoop/examples/meteo/web/index.html
