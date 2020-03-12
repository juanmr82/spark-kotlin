package com.pybsoft.spark.sql.examples

import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.desc


import java.io.File
import java.net.URL
import java.nio.charset.Charset


const val NASA_SITES_URL = "https://data.nasa.gov/resource/gvk9-iz74.json"
const val APP_NAME = "SimpleSqlKotlin"
const val SITES_SCHEMA = "location-schema.json"
const val NASA_SITES_FILE = "nasa-sites.json"

fun main(args: Array<String>) {

    //Read the sites JSON objects from the NASA Sites Api URL and write it into a file
    val jsonString = URL(NASA_SITES_URL).readText(Charset.forName("UTF-8"))
    File(NASA_SITES_FILE).writeText(jsonString)


    //Create an spark session
    val session = SparkSession.builder()
            .appName(APP_NAME)
            //Limit the shuffle partitions in this case for 5
            .config("spark.sql.shuffle.partitions",5)
            //Local spark application
            .master("local[*]")
            .orCreate

    //Read the schema json frile from the resources path and create the {@link org.apache.spark.sql.types.SructType} object. Schema JSON has been previously created
    val jsonSchemaString = ClassLoader.getSystemResource(SITES_SCHEMA).readText()
    val locationsSchema =  DataType.fromJson(jsonSchemaString) as StructType


    //Read the Nasa sites json file created at the beginning using the specified schema. Json file contains eol after each object
    val rawSitesDataset = session.read()
            .option("mode","PERMISSIVE")
            .option("multiLine",true)
            .schema(locationsSchema)
            .json(NASA_SITES_FILE)


    //Get the latitude and longitude as single columns from the location embedded object. Cache the dataset
    val sitesDataset = rawSitesDataset.withColumn("latitude",Column("location.latitude"))
            .withColumn("longitude", Column("location.longitude"))
            .drop("location")
            .cache()

    //Show the number of sites per center and sort them in descending order. Use aggregate count function
    sitesDataset.groupBy("center").count().withColumnRenamed("count","nr_sites").sort(desc("nr_sites")).show()

    //SHow only sites with an URL defined
    sitesDataset.filter(Column("url_link").isNotNull.and(Column("status").equalTo("Active"))).show(false)

    //Show sites with last_update year greater or equal than 2013 and status active. Using an string expression
    sitesDataset.filter("status='Active' and last_update >= '2013'").sort(desc("last_update")).show(100,false)

    //Shows site contact name and phone for each center, using sql
    sitesDataset.createOrReplaceTempView("nasa_sites")
    session.sql("SELECT center,contact,phone,zipcode FROM nasa_sites WHERE status='Active' GROUP BY center,contact,phone,zipcode ORDER BY center").show(false)

    //Close session
    session.close()

    //Delete file
    File(NASA_SITES_FILE).delete()


}

