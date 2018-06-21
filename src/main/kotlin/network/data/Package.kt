import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.util.*

data class Message(val topic: String, val type: String, val payload: Any?) {
    companion object {
        private val mapper = ObjectMapper().registerModule(KotlinModule())

        fun serializePayload(message: Message): Message {
            var serializedPayload: ByteArray? = null

            try {
                serializedPayload = mapper.writeValueAsBytes(message.payload)
            } catch (e: JsonParseException) {
                println("Unable to serialize payload: ${message.payload} for message: $message")
            }

            return Message(message.topic, message.type, serializedPayload)
        }

        fun <T> deserializePayload(message: Message, clazz: Class<T>): Message {
            var deserializedPayload: T? = null

            try {
                deserializedPayload = mapper.readValue(message.payload as ByteArray, clazz)
            } catch (e: JsonParseException) {
                println("Unable to deserialize payload: ${message.payload} for message: $message")
            }

            return Message(message.topic, message.type, deserializedPayload)
        }
    }
}
data class PackageMetadata(val port: Int, val timestampsec: Long = Date().time)

class Package(val message: Message, val metadata: PackageMetadata) {
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
        val pack = Package.serialize(this)

        return if (pack != null)
            "[Package]: ${pack.toString(StandardCharsets.UTF_8)}"
        else
            "[Package]: null reference"
    }
}

data class Address(val host: String, val port: Int) {
    fun getInetAddress(): InetAddress {
        return InetAddress.getByName(host)
    }
}

abstract class Payload