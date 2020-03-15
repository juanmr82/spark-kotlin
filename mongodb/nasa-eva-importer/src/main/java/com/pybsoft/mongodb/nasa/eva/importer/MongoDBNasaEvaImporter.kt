package com.pybsoft.mongodb.nasa.eva.importer

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.int
import com.mongodb.BasicDBObject
import com.mongodb.MongoClient
import com.pybsoft.mongodb.nasa.eva.importer.model.EVA
import org.litote.kmongo.KMongo
import org.litote.kmongo.getCollection
import org.slf4j.LoggerFactory
import java.net.URL

/**
 * This class is where the MongoDB importer is implemented. The lightweight and Kotlin Native library KMongo is used
 * to map the objects to MongoDB documents
 *
 * The arguments to run the importer are
 * @param --mongodb-host. Mongo host. Default is localhost
 * @param --mongdb-port. Mongo Port. Default is 27017
 * @param --db. Existing Mongo Database name. Required
 * @param --collection. Existing Database Collection Name. Required
 * @param --drop. If specified, all existing documents in the given collection will be erased
 * @param --eva-api. asa Open Data Portal EVA Api endpoint. Default is https://data.nasa.gov/resource/eva.json
 *
 * @author Juan Reina (juanmr82@gmail.com)
 */
class MongoDBNasaEvaImporter : CliktCommand(), AutoCloseable {

    private val logger = LoggerFactory.getLogger(this::class.java)
    private val defaultEvaApi = "https://data.nasa.gov/resource/eva.json"

    private val mongodbHost: String by option("--mongodb-host", help = "Mongo host. Default is localhost").default("localhost")
    private val mongodbPort: Int by option("--mongodb-port", help = "Mongo port. Default is 27017").int().default(27017)
    private val dataBase: String by option("--db", help = "Database to connect to").required()
    private val collection: String by option("--collection", help = "Collection to connect to").required()
    private val dropDb: Boolean by option("--drop", help = "Drop DB before importing").flag(default = false)
    private val evaApi: String by option("--eva-api", help = "Nasa Open Data Portal EVA Api endpoint").default(defaultEvaApi)

    private lateinit var mongoClient: MongoClient

    override fun run() {

        logger.info("Starting EVA MongoDB importer")

        val mapper = jacksonObjectMapper()

        logger.info("Reading objects from endpoint $evaApi")
        val evas = mapper.readValue<List<EVA>>(URL(evaApi))

        if (evas.isEmpty()) {
            logger.error("No EVA data was retrieved from the endpoint $evaApi")
            return
        }
        logger.info("Reading done. A total number of ${evas.size} objects were fetched")

        logger.info("Creating client to connect to server with url $mongodbHost:$mongodbPort")
        mongoClient = KMongo.createClient(mongodbHost, mongodbPort)

        logger.info("Connecting to Database $dataBase and Collection $collection")
        val dataBase = mongoClient.getDatabase(dataBase)
        val evaCollection = dataBase.getCollection<EVA>(collection)

        if (dropDb) {
            logger.info("Deleting all objects from $collection")
            evaCollection.deleteMany(BasicDBObject())
        }

        logger.info("Writing fetched EVA records into Collection $collection")
        evaCollection.insertMany(evas)
    }

    override fun close() {
        logger.info("Finishing importer")
        logger.info("Closing MongoDB client connection")
        mongoClient.close()
    }

}