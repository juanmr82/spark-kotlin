# Basic Spark Sql

This module uses the NASA's Facilities Dataset (https://data.nasa.gov/Management-Operations/NASA-Facilities/gvk9-iz74) 
to do some basic processing using Apache Spark with Kotlin

The Dataset is fetched via the The Socrata Open Data API (SODA) facilities endpoint, stored in a JSON file which is then 
loaded into a spark dataset

The schema of the Dataset has been stored within the resource directory of the module as a JSON file
