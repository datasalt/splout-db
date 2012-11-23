Example data from "Daily Global Weather Measurements, 1929-2009 (NCDC,
GSOD)" http://aws.amazon.com/datasets/2759

**This data set can only be used within the United States. If you redistribute 
any of these data to others, you must include this same notification.**

Schema:
-------

stn:int,wban:int,year:int,month:int,day:int,date:int,temp:double,temp_observations:int,dewp:double,dewp_observations:int,slp:double,slp_observations:int,stp:double,stp_observations:int,visib:double,visib_observations:int,wdsp:double,wdsp_observations:int,mxspd:double,gust:double,max:double,max_flag:string,min:double,min_flag:string,prcp:double,prcp_flag:string,sndp:double,frshtt:int

Generate meteo table:
--------------------- 

--input examples/meteo/meteo.txt --output database-files --tablespace meteo_table --table meteo --schema "stn:int,wban:int,year:int,month:int,day:int,date:int,temp:double,temp_observations:int,dewp:double,dewp_observations:int,slp:double,slp_observations:int,stp:double,stp_observations:int,visib:double,visib_observations:int,wdsp:double,wdsp_observations:int,mxspd:double,gust:double,max:double,max_flag:string,min:double,min_flag:string,prcp:double,prcp_flag:string,sndp:double,frshtt:int" --partitionby stn,wban --partitions 4 --skipheading --fixedwidthfields "0,5,7,11,14,17,18,19,20,21,14,21,24,29,31,32,35,40,42,43,46,51,53,54,57,62,64,65,68,72,74,75,78,82,84,85,88,92,95,99,102,107,108,108,110,115,116,116,118,122,123,123,125,129,132,137" 

--root database-files --tablespaces meteo_table --replication 2 --qnode http://localhost:4412

Meteo from definition file:
---------------------------
GeneratorCMD -tf examples/meteo/meteo-pby-stn-wban.json -tf examples/meteo/meteo-pby-date.json -o database-files
DeployerCMD --qnode http://localhost:4412 --root database-files --tablespaces meteo-pby-stn-wban