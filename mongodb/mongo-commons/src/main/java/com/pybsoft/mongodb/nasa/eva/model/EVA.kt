package com.pybsoft.mongodb.nasa.eva.model

/**
 * Extra Vehicular Activity Data Class.
 *
 *  POJO to be mapped into MongoDB and to be used as schema in for Spark Dataset
 *
 *
 * @author Juan Reina (juanmr82@gmail.com)
 */

data class EVA(
        var eva: Int?,
        var country: String?,
        var crew: String?,
        var vehicle: String?,
        var date: String?,
        var duration: String?,
        var purpose: String?
) {
    /**
     *The empty constructor is necessary to be defined when using Encoders in Spark
     */
    constructor() : this(eva = null, country = null, crew = null, vehicle = null, date = null, duration = null, purpose = null)
}