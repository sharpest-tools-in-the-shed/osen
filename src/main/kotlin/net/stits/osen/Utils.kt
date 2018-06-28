package net.stits.osen

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.util.*
import java.util.logging.Logger


data class Session(val id: Int)

/**
 * For now it's just a port on which we are listening to (so remote peer can send us messages too) but maybe sometime here will be more
 */
data class PackageMetadata(val port: Int, val session: Session? = null)

/**
 * Just a wrapper around InetAddress
 */
data class Address(val host: String, val port: Int) {
    fun getInetAddress(): InetAddress {
        return InetAddress.getByName(host)
    }
}

/**
 * Class that represents user-defined payload of UPD-packet.
 * Sending this message via P2P.send() will trigger @On-annotated method on remote peer.
 * Topic and Type are used to define some namespaces (Topic is high level, Type is low level). Lets suppose you need to
 * implement some protocol - you should use protocol identifier (e.g. name) as Topic and protocol-specific message identifiers
 * as Types. Example: you need to implement Kademlia protocol, it has 4 types of messages (store, find_node, find_value, ping),
 * so your topic can be like "KAD" and types can be like "STORE", "FIND_NODE", "FIND_VALUE", "PING" and maybe "PONG" :)
 *
 * @param topic {String} nothing to say - Message Topic
 * @param type {String} same as topic, but Message Type
 * @param payload {Any?} any payload you want to send (if null - no payload will be sent). It would be nice to create
 * data class for each payload type you send. If remote peer has mismatched payload parameter in its @On method it will throw JsonParseException.
 */
data class Message(val topic: String, val type: String, val payload: Any? = null) {
    private val mapper = ObjectMapper().registerModule(KotlinModule())

    companion object {
        val logger = loggerFor(Message::class.java)
    }

    /**
     * Serializes payload of this message. Cause: Any serializes as ByteString (in my case for some reason), so we need to
     * manually serialize it to ByteArray and then manually deserialize in needed type.
     */
    fun serialize(): SerializedMessage {
        var serializedPayload = mapper.writeValueAsBytes(payload)

        if (serializedPayload == null)
            serializedPayload = ByteArray(0)

        return SerializedMessage(topic, type, serializedPayload)
    }
}

/**
 * Message with serialized payload. For inner usage only.
 */
data class SerializedMessage(val topic: String, val type: String, val payload: ByteArray) {
    private val mapper = ObjectMapper().registerModule(KotlinModule())

    companion object {
        val logger = loggerFor(SerializedMessage::class.java)
    }

    /**
     * Deserializes payload in given class
     *
     * @param clazz {Class<T>} class to deserialize
     */
    fun <T> deserialize(clazz: Class<T>): Message? {
        logger.info("Deserializing payload $payload to type ${clazz.canonicalName}")
        val deserializedPayload = mapper.readValue(payload, clazz)

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

/**
 * Abstraction on UPD-packet. The main reason is to attach some semantic meta-information to it.
 */
class Package(val message: SerializedMessage, val metadata: PackageMetadata) {
    companion object {
        private val mapper = ObjectMapper().registerModule(KotlinModule())
        val logger = loggerFor(Package::class.java)

        /**
         * Deserializes from bytes
         */
        fun deserialize(jsonBytes: ByteArray): Package? {
            var pack: Package? = null
            try {
                pack = mapper.readValue(jsonBytes, object : TypeReference<Package>() {})
            } catch (e: JsonParseException) {
                logger.warning("Unable to parse json from bytes: ${jsonBytes.toString(StandardCharsets.UTF_8)}")
            }

            return pack
        }

        /**
         * Serializes in bytes
         */
        fun serialize(pkg: Package): ByteArray? {
            var bytes: ByteArray? = null
            try {
                bytes = mapper.writeValueAsBytes(pkg)
            } catch (e: JsonMappingException) {
                logger.warning("Unable to map package to json: $pkg")
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

/**
 * Creates simple logger
 */
fun <T> loggerFor(clazz: Class<T>) = Logger.getLogger(clazz.canonicalName)