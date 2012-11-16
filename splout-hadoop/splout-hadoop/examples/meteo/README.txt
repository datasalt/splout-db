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

0,5,7,11,14,17,18,19,20,21,14,21,24,29,31,32,35,40,42,43,46,51,53,54,57,62,64,65,68,72,74,75,78,82,84,85,88,92,95,99,102,107,108,108,110,115,116,116,118,122,123,123,125,129,132,137

STN---  1-6       Int.   Station number (WMO/DATSAV3 number)
WBAN    8-12      Int.   WBAN number where applicable--this is the
YEAR    15-18     Int.   The year.
MODA    19-22     Int.   The month and day.
TEMP    25-30     Real   Mean temperature for the day in degrees
Count   32-33     Int.   Number of observations used in 
DEWP    36-41     Real   Mean dew point for the day in degrees
Count   43-44     Int.   Number of observations used in 
SLP     47-52     Real   Mean sea level pressure for the day
Count   54-55     Int.   Number of observations used in 
STP     58-63     Real   Mean station pressure for the day
Count   65-66     Int.   Number of observations used in 
VISIB   69-73     Real   Mean visibility for the day in miles
Count   75-76     Int.   Number of observations used in 
WDSP    79-83     Real   Mean wind speed for the day in knots
Count   85-86     Int.   Number of observations used in 
MXSPD   89-93     Real   Maximum sustained wind speed reported 
GUST    96-100    Real   Maximum wind gust reported for the day
MAX     103-108   Real   Maximum temperature reported during the 
Flag    109-109   Char   Blank indicates max temp was taken from the
MIN     111-116   Real   Minimum temperature reported during the 
Flag    117-117   Char   Blank indicates min temp was taken from the
PRCP    119-123   Real   Total precipitation (rain and/or melted
Flag    124-124   Char   A = 1 report of 6-hour precipitation 
SNDP    126-130   Real   Snow depth in inches to tenths--last     
FRSHTT  133-138   Int.   Indicators (1 = yes, 0 = no/not          
