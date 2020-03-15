package com.pybsoft.mongodb.nasa.eva.importer

/**
 * Main function. Arguments are passes to the importer and it is executed
 *
 * @author Juan Reina (juanmr82@gmail.com)
 */
fun main(args: Array<String>) {

    MongoDBNasaEvaImporter().use { it.main(args) }

}