package net.stits.osen

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import java.util.logging.Logger


/**
 * Creates simple logger
 */
inline fun <reified T> loggerFor(): Logger = Logger.getLogger(T::class.java.canonicalName)

/**
 * Static class that makes serialization easier
 */
class SerializationUtils {
    companion object {
        val mapper = ObjectMapper().registerModule(KotlinModule())

        fun anyToJSON(obj: Any) = mapper.writeValueAsString(obj)
        fun anyToBytes(obj: Any) = mapper.writeValueAsBytes(obj)

        inline fun <reified T> jSONToAny(json: String): T? = mapper.readValue(json, T::class.java)
        inline fun <reified T> bytesToAny(bytes: ByteArray): T? = mapper.readValue(bytes, T::class.java)

        fun <T> jSONToAny(json: String, clazz: Class<T>): T? = mapper.readValue(json, clazz)
        fun <T> bytesToAny(bytes: ByteArray, clazz: Class<T>): T? = mapper.readValue(bytes, clazz)
    }
}