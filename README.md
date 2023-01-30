# trg-marinos-assessment

## Summary
This project is an assessment between  TRG Research & Development and Marinos as candidate.

This scala spark project is built with Maven and runs an ETL pipeline that loads csv files, joins them, save a silver table,
create KPIs and finally serves the data via endpoints.

## Description
### Input data
The datasets are about UK reported street crimes from June 2022 to November 2022.

### Store data
The input data have been stored manually under **resources** folder inside the project.

### Table construction
Below you can find the necessary steps for the silver table creation: 
* Load the street crimes data
* Load the crime outcomes data
* Construct the requested table by 
  * LEFT join the street data with the outcomes data on **Crime ID** column
  * Add **districtName** column by using the *input_file_name* spark function to get the file path,
  split on **/** character, get the last element of the result array, extract file name given a regex 
  and then replace the **-** with white space
  * Rename the columns **Crime ID** to crimeID, **Crime type** to crimeType and lower case the Latitude and Longitude
  columns
  * Cast latitude column to DECIMAL(8,6) because Lat can only be between -90 and 90 degrees with 6 precision
  * Cast longitude column to DECIMAL(8,6) because Lat can only be between -180 and 180 degrees with 6 precision

### KPIs
Based on the requested table there are some useful KPIs that we can extract:
* different crimes: this kpi shows what crime types occurred in 6 months period
* different outcomes: this kpi shows what are the crime outcomes in 6 months period
* crimes by location: this kpi shows the number of crimes per location in 6 months period
* crimes by type: this kpi shows the number of crimes per crime type in 6 months period
* crimes by outcome: this kpi shows the number of crimes per crime outcome in 6 months period
* top crimes per location: this kpi shows the top 3 crime types per location
* outcome status per location: this kpi creates 3 statuses based on crime outcomes
  * UNRESOLVED: Unable to prosecute suspect
  * PENDING: Under investigation
  * RESOLVED: the rest of the outcomes
  and shows the number of statuses per location
* top outcome by type: this kpi shows the top crime outcome per crime type

## Applications
There are 2 application files in this project.

The first one describes the ETLPipeline and the other the Download the silver table as csv

### ETLPipeline
The pipeline described in the **ETLPipeline** file consists of the following steps:
* Create a standalone spark cluster with one master node
* Suppress all the logs that are not ERROR level
* Define the directory where our parquet files will be saved
* If the directory does not exist then proceed 
  * Load as Spark DataFrame all the files for street crimes using a wildcard
  * Load as Spark DataFrame all the files for crime outcomes using a wildcard
  * Construct the requested table by
    * LEFT join the street data with the outcomes data on **Crime ID** column
    * Add **districtName** column by using the *input_file_name* spark function to get the file path,
      split on **/** character, get the last element of the result array, extract file name given a regex
      and then replace the **-** with white space
    * Rename the columns **Crime ID** to crimeID, **Crime type** to crimeType and lower case the Latitude and Longitude
      columns
    * Cast latitude column to DECIMAL(8,6) because Lat can only be between -90 and 90 degrees with 6 precision
    * Cast longitude column to DECIMAL(8,6) because Lat can only be between -180 and 180 degrees with 6 precision
  * Saves the constructed dataframe as parquet file with mode overwrite by dropping any duplicates, coalescing the data partitions to 1, because
    the parquet files are below 200MB.
* Start HTTP Server to serve the data on ```http://localhost:9000/```

#### HTTPServer
The com.twitter.finagle-http server has been used for accessing the data and the kpis.
The available endpoints are:
* For crimes silver table ```http://localhost:9000/?table=crimes``` *(limits to 1000 rows for GC issues)*
* For different crimes ```http://localhost:9000/?table=differentCrimes```
* For different outcomes ```http://localhost:9000/?table=differentOutcomes```
* For crimes by location ```http://localhost:9000/?table=crimesByLocation```
* For crimes by type ```http://localhost:9000/?table=crimesByType```
* For crimes by outcome ```http://localhost:9000/?table=crimesByOutcome```
* For top crimes per location ```http://localhost:9000/?table=topCrimesPerLocation```
* For outcome status per location ```http://localhost:9000/?table=outcomeStatusPerLocation```
* For top outcome by type ```http://localhost:9000/?table=topCrimeOutcomesPerType```

### DownloadDataAsCSV
In this file we check if the silver table has been created and then save the data as csv on the **output/crimes_data** directory

## Launch a Spark Application
In order to run the **ETLPipeline** you have to:
* ```cd trg-marinos-assessment```
* ```mvn package exec:java -Dexec.mainClass=trg.data.apps.ETLPipeline```

In order to run the **DownloadDataAsCSV** you have to:
* ```cd trg-marinos-assessment```
* ```mvn package exec:java -Dexec.mainClass=trg.data.apps.DownloadDataAsCSV```
