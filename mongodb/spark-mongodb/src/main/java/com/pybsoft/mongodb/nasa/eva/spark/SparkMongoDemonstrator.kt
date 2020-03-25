package com.pybsoft.mongodb.nasa.eva.spark

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.int
import com.mongodb.spark.MongoSpark
import com.pybsoft.mongodb.nasa.eva.model.EVA
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.DataTypes
import org.slf4j.LoggerFactory

/**
 * <p> It runs a simple Spark-MongoDB connector Demonstrator. The class implements the {@see com.github.ajalt.clikt.core.CliktCommand}
 * class, which takes care of parsing the input parameters. Different statistics on NASA EVA information is written within an user
 * defined collection</p>
 * <br>
 * <p>The NASA EVA data should be imported into a MongoDB instance using the {@see com.pybsoft.mongodb.nasa.eva.importer.MongoDBNasaEvaImporter}
 * class defined within this project</p>
 *
 * @author Juan Reina (juanmr82@gmail.com)
 *
 * @param --mongodb-host. MongoDB instance host name or address. Default is localhost
 * @param --mongodb-port. MongoDB instance port. Default is 27017
 * @param --db. Database to connect to. Required
 * @param --src-collection. Collection to read the EVA data from. Required
 * @param --results-collection. Collection where to store the results. Required
 */
class SparkMongoDemonstrator : CliktCommand(), AutoCloseable {

    private val logger = LoggerFactory.getLogger(this::class.java)

    private val mongodbHost: String by option("--mongodb-host", help = "Mongo host. Default is localhost").default("localhost")
    private val mongodbPort: Int by option("--mongodb-port", help = "Mongo port. Default is 27017").int().default(27017)
    private val dataBase: String by option("--db", help = "Database to connect to").required()
    private val srcCollection: String by option("--src-collection", help = "Collection to read data from").required()
    private val resultCollection: String by option("--results-collection", help = "Collection where to store results").required()

    private lateinit var session: SparkSession

    private val UDF_NAME = "convert_ts"

    private object Columns {
        const val CREW_MEMBERS = "crew_members"
        const val NR_ISS_EVAS = "nr_iss_evas"
        const val DURATION_SECONDS = "duration_seconds"
    }


    /**
     * Execution starts here
     */
    override fun run() {

        logger.info("Creating Spark Session to be connected to mongodb url $mongodbPort:$mongodbPort")
        logger.info("Input Collection Path: $dataBase.$srcCollection")
        logger.info("Output Collection Path: $dataBase.$resultCollection")

        session = SparkSession.builder()
                .master("local[*]")
                .appName("MongoDBSparkEVA")
                .config("spark.sql.shuffle.partitions", 5)
                .config("spark.mongodb.input.uri", "mongodb://$mongodbHost:$mongodbPort/$dataBase.$srcCollection")
                .config("spark.mongodb.output.uri", "mongodb://$mongodbHost:$mongodbPort/$dataBase.$resultCollection")
                .orCreate


        //Register the UDF to convert duration string into seconds
        session.sqlContext().udf().register(UDF_NAME, createDurationToTSUDF(), DataTypes.LongType)

        //Only interested in ISS based EVAs
        val evasDataSet = MongoSpark.load(JavaSparkContext(session.sparkContext()))
                //Construct the dataset. We use the bean class EVA to give a small example of how to use Dataset API
                .toDS(EVA::class.java)
                //Filter the initial Dataset
                .filter { eva -> eva.vehicle != null && eva.vehicle!!.contains("ISS") }

        //The crew members are separated by several whitespaces (if more than 1). Convert this column into an array
        //At this point, since we create a new column, we switch from a Dataset<EVA> to a Dataset<Row> (aka Dataframe)
        val evasFinalDS = evasDataSet.withColumn(Columns.CREW_MEMBERS, split(Column(EVA::crew.name), "\\s{2,}")
                .cast("array<String>"))
                .drop(Column(EVA::crew.name))
                .withColumn(EVA::crew.name, explode(Column(Columns.CREW_MEMBERS)))
                .drop(Columns.CREW_MEMBERS)
                .filter(Column(EVA::crew.name).notEqual(""))
                .withColumn(Columns.DURATION_SECONDS, callUDF(UDF_NAME, Column(EVA::duration.name)).alias("avg_duration"))
                .cache()

        val maxAvgMinDF = evasFinalDS.groupBy(EVA::crew.name)
                .agg(
                        count(EVA::crew.name).alias(Columns.NR_ISS_EVAS),
                        max(Columns.DURATION_SECONDS).alias("max_duration"),
                        round(avg(Columns.DURATION_SECONDS)).alias("average_duration"),
                        min(Columns.DURATION_SECONDS).alias("min_duration")
                ).sort(desc(Columns.NR_ISS_EVAS))

        logger.info("Writing Min, Max and Avg duration in seconds Stats on EVAS per crew member into MongoDB")
        //Save Simple stats Dataframe
        MongoSpark.save(maxAvgMinDF)

        //Removing Dataframe from cache
        evasFinalDS.unpersist()

    }

    /**
     * Returns a single parameter and return UDF (User Defined Function) {@see org.apache.spark.sql.UDF1} transforming
     * the he duration string with format HH::mm into seconds.
     * <br>
     * @return UDF1 to transform the column with format HH:mm into Long
     */
    private fun createDurationToTSUDF(): UDF1<String?, Long> {

        //If null or format incorrect, return 0 as Long
        return UDF1 { durationString: String? ->
            if (durationString == null) {
                return@UDF1 0L
            } else {
                val hourAndMinute = durationString.split(":")
                if (hourAndMinute.size < 2) {
                    return@UDF1 0L
                } else {
                    return@UDF1 (Integer.parseInt(hourAndMinute[0]) * 3600 + Integer.parseInt(hourAndMinute[1]) * 60).toLong()
                }
            }
        }
    }


    override fun close() {
        logger.info("Finishing pipeline. Closing session")
        session.close()
    }
}