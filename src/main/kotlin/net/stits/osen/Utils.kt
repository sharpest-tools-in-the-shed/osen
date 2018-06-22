package net.stits.osen

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.util.*


data class PackageMetadata(val port: Int)

interface Payload

data class Address(val host: String, val port: Int) {
    fun getInetAddress(): InetAddress {
        return InetAddress.getByName(host)
    }
}

data class Message(val topic: String, val type: String, val payload: Any?) {
    private val mapper = ObjectMapper().registerModule(KotlinModule())

    fun serialize(): SerializedMessage {
        var serializedPayload: ByteArray? = null

        try {
            serializedPayload = mapper.writeValueAsBytes(payload)
        } catch (e: JsonParseException) {
            println("Unable to serialize payload: $payload} for message: $this")
        }

        if (serializedPayload == null)
            serializedPayload = ByteArray(0)

        return SerializedMessage(topic, type, serializedPayload)
    }
}

data class SerializedMessage(val topic: String, val type: String, val payload: ByteArray) {
    private val mapper = ObjectMapper().registerModule(KotlinModule())

    fun <T> deserialize(clazz: Class<T>): Message {
        var deserializedPayload: T? = null

        try {
            deserializedPayload = mapper.readValue(payload, clazz)
        } catch (e: JsonParseException) {
            println("Unable to deserialize payload: $payload for message: $this")
        }

        return Message(topic, type, deserializedPayload)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as SerializedMessage

        if (topic != other.topic) return false
        if (type != other.type) return false
        if (!Arrays.equals(payload, other.payload)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = topic.hashCode()
        result = 31 * result + type.hashCode()
        result = 31 * result + Arrays.hashCode(payload)
        return result
    }
}

class Package(val message: SerializedMessage, val metadata: PackageMetadata) {
    companion object {
        private val mapper = ObjectMapper().registerModule(KotlinModule())

        fun deserialize(jsonBytes: ByteArray): Package? {
            var pack: Package? = null
            try {
                pack = mapper.readValue(jsonBytes, object : TypeReference<Package>() {})
            } catch (e: JsonParseException) {
                println("Unable to parse json from bytes: ${jsonBytes.toString(StandardCharsets.UTF_8)}")
            }

            return pack
        }

        fun serialize(pkg: Package): ByteArray? {
            var bytes: ByteArray? = null
            try {
                bytes = mapper.writeValueAsBytes(pkg)
            } catch (e: JsonMappingException) {
                println("Unable to map package to json: $pkg")
            }

            return bytes
        }
    }

    override fun toString(): String {
        val pack = serialize(this)

        return if (pack != null)
            "[Package]: ${pack.toString(StandardCharsets.UTF_8)}"
        else
            "[Package]: null reference"
    }
}

fun drawBanner() {
    println(
                    "#######  #####  ####### #     # \n" +
                    "#     # #     # #       ##    # \n" +
                    "#     # #       #       # #   # \n" +
                    "#     #  #####  #####   #  #  # \n" +
                    "#     #       # #       #   # # \n" +
                    "#     # #     # #       #    ## \n" +
                    "#######  #####  ####### #     # \n" +
                    "--- P2P messaging framework --- \n"
    )
}