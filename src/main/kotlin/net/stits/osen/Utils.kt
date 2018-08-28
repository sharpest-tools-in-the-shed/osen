package net.stits.osen

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import java.util.logging.Logger
import java.util.zip.Deflater
import java.util.zip.Inflater


/**
 * Creates simple logger
 */
inline fun <reified T> loggerFor(): Logger = Logger.getLogger(T::class.java.canonicalName)

/**
 * Static class that makes serialization easier
 */
class SerializationUtils {
    companion object {
        val mapper: ObjectMapper = ObjectMapper().registerModule(KotlinModule())

        fun anyToJSON(obj: Any): String = mapper.writeValueAsString(obj)
        fun anyToBytes(obj: Any): ByteArray = mapper.writeValueAsBytes(obj)

        inline fun <reified T : Any> jSONToAny(json: String): T = mapper.readValue(json, T::class.java)
        inline fun <reified T : Any> bytesToAny(bytes: ByteArray): T = mapper.readValue(bytes, T::class.java)

        fun <T : Any> jSONToAny(json: String, clazz: Class<T>): T = mapper.readValue(json, clazz)
        fun <T : Any> bytesToAny(bytes: ByteArray, clazz: Class<T>): T = mapper.readValue(bytes, clazz)
    }
}

class CompressionUtils {
    companion object {
        fun compress(data: ByteArray): ByteArray {
            val deflater = Deflater(Deflater.BEST_SPEED)
            deflater.setInput(data)
            deflater.finish()

            val output = ByteArray(data.size)
            val size = deflater.deflate(output)
            deflater.end()

            return output.copyOfRange(0, size)
        }

        fun decompress(data: ByteArray): ByteArray {
            val inflater = Inflater()
            inflater.setInput(data)

            val output = ByteArray(data.size * 10)
            val size = inflater.inflate(output)
            inflater.end()

            return output.copyOfRange(0, size)
        }
    }
}