package com.pybsoft.mongodb.nasa.eva.importer.model

import java.time.Duration
import java.time.LocalDateTime
/**
 * Extra Vehicular Activity Data Class.
 *
 * POJO to be mapped into MongoDB
 *
 * @author Juan Reina (juanmr82@gmail.com)
 */

data class EVA(
        val eva:Int?,
        val country:String?,
        val crew:String?,
        val vehicle:String?,
        val date:String?,
        val duration:String?,
        val purpose: String?
)