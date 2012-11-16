Starting Splout SQL:
====================

This example assumes you have already started a local QNode and DNode as shown in [splout-server](). 

Upload to HDFS the examples folder, as it contains the input files. 

<pre>
	hadoop fs -put examples examples
</pre>

Single Table Loading:
=====================

Load the table city from the CSV file city.csv and use the country_code field to partition it into 4 partitions:  

<pre>
	hadoop jar splout-*-hadoop.jar simple-generate --input examples/world/city.csv --output database-files --tablespace city_pby_country_code --table city --separator , --escape \\ --quotes \"\"\" --nullstring \\N --schema "id:int,name:string,country_code:string,district:string,population:int" --partitionby country_code --partitions 4
</pre>

You could have also used full qualified paths (replace /user/ivan by your home folder in Hadoop):

<pre>
	hadoop jar splout-*-hadoop.jar simple-generate --input hdfs:///user/ivan/examples/world/city.csv --output hdfs:///user/ivan/database-files --tablespace city_pby_country_code --table city --separator , --escape \\ --quotes \"\"\" --nullstring \\N --schema "id:int,name:string,country_code:string,district:string,population:int" --partitionby country_code --partitions 4
</pre>

Finally, deploy the results into Splout, using replication 2. That is, there will be 2 replicas per each partition:

<pre>
	hadoop jar splout-*-hadoop.jar deploy --root database-files --tablespaces city_pby_country_code --replication 2 --qnode http://localhost:4412
</pre>

In this particular example, as you have only one dnode, replication will be adjusted automatically to 1. 

Look at the panel to see the information of the created tablespace:

<pre>
	http://localhost:4412/tablespace.html?tablespace=city_pby_country_code
</pre>

Particularly, look at how partitions has been distributed using different country_code ranges. 

Now, you can launch some queries at the query console:

<pre>
	http://localhost:4412/console.html
</pre>

Perform the following query to show all the tables in the tablespace:

<pre>
  Tablespace = city_pby_country_code
  Partition key = 
  Query = SELECT * FROM sqlite_master WHERE type='table';
</pre>

Now, let's find the cities  with country_code 

<pre>
  Tablespace = city_pby_country_code
  Partition key = JPN 
  Query = SELECT * FROM city WHERE country_code = "JPN"
</pre>

Note that the shard 2 (partition 2) was hit. That is because we provided "JPN" as partition key. Otherwise, an improper partition would have been hit, and no results would have been presented. Perform a test. Repeat the query, but keep the partition key empty. You'll see no results and partition 0 being hit.

The former query can be performed just using the REST Splout SQL interface. For the former query the URL would be the following:

<pre>
	http://localhost:4412/api/query/city_pby_country_code?key=JPN&sql=SELECT%20*%20FROM%20city%20WHERE%20country_code%20%3D%20%22JPN%22
</pre>

Multiple Tables:
===============

Multiple tables per tablespace are allowed. But all of them must be partitioned by the same key. 
We are going to create two different tablespaces:

world-pby-country:
------------------
country -> partitioned by country
city -> partitioned by country_code
country_language -> full table present in all partitions

world-pby-continent-region:
------------------------
country -> partitioned by continent and region
country_language -> full table present in all partitions

The structure of these tablespaces, and the the specification of the input data are in the files: 

<pre>
	examples/world/world-pby-country.json
	examples/world/world-pby-continent-region.json
</pre>

Launch the following command to create both tablespaces:

<pre>
	hadoop jar splout-*-hadoop.jar generate --output database-files --tablespacefile examples/world/world-pby-country.json --tablespacefile examples/world/world-pby-continent-region.json
</pre>

Have a look to the database-files folder:

<pre>
	hadoop fs -ls database-files
</pre>

At this point, the tablespace files has been created, but they are not still present at Splout SQL. We have to deploy them. We can deploy both tablespaces at the same time atomically, so we can be sure that information between tablespaces will be consistent. Whichever of the following command performs the deploy of the tablespaces. 

<pre>
	hadoop jar splout-*-hadoop.jar deploy -root database-files -ts world-pby-continent-region -ts world-pby-country -r 2 -q http://localhost:4412
	hadoop jar splout-*-hadoop.jar deploy --config-file examples/world/deployment.json --qnode http://localhost:4412
</pre>

Now both tablespaces should be present at Splout SQL. Have a look to the console. 

You can check the following queries:

All Japan Languages:

<pre>
  Tablespace = world-pby-country
  Partition key = JPN 
  Query = SELECT country_language.* FROM country, country_language WHERE country.code = country_language.country_code AND country.code = "JPN"
</pre>

Distinct districs in Japan:

<pre>
  Tablespace = world-pby-country
  Partition key = JPN 
  Query = SELECT country.name, count(distinct district) as num_districts FROM country, city WHERE country.code = "JPN" and country.code = city.country_code;
</pre>

Population of Western Europe:

<pre>
  Tablespace = world-pby-continent-region
  Partition key = EuropeWestern Europe 
  Query = SELECT continent,region,sum(population) total_population FROM country WHERE continent = "Europe" AND region = "Western Europe";
</pre>

Biggest countries on Central Africa:

<pre>
  Tablespace = world-pby-continent-region
  Partition key = AfricaCentral Africa
  Query = SELECT name, surface_area FROM country WHERE continent = "Africa" AND region = "Central Africa" ORDER BY surface_area DESC;
</pre>

Most talked languages on Central Africa:

<pre>
  Tablespace = world-pby-continent-region
  Partition key = AfricaCentral Africa
  Query = SELECT  language, sum((percentage/100)*population) as people FROM country, country_language WHERE country.code = country_language.country_code AND continent = "Africa" AND region = "Central Africa" GROUP BY language ORDER BY people DESC;
</pre>

Note that all the queries presented before are compatible with the partitioning decided for the tablespace. Otherwise they would have not worked properly. Also, be careful to properly define the needed indexes in order to answer queries fast.
