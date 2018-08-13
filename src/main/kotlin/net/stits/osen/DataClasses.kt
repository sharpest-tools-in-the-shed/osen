package net.stits.osen

import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.util.*


const val MAX_PACKET_SIZE_BYTES = 1024

// TODO: this concept can be more generic allowing more complex flow to be implemented
object SessionStage {
    const val REQUEST = "REQUEST"
    const val RESPONSE = "RESPONSE"
    const val CONSUMED = "CONSUMED"
    const val INACTIVE = "INACTIVE"

    fun next(stage: String): String {
        return when (stage) {
            REQUEST -> RESPONSE
            RESPONSE -> CONSUMED
            CONSUMED -> CONSUMED
            else -> INACTIVE
        }
    }
}

/**
 * This class incapsulates microsession for request-response interaction model. It enables us to know what method should be invoked and when.
 */
data class Session(val id: Int, private var stage: String = SessionStage.REQUEST) {
    fun processLifecycle() {
        check(stage != SessionStage.CONSUMED) { "Session $id is expired. Unable to switch stage." }
        check(stage != SessionStage.INACTIVE) { "Unable to process inactive stage of session $id" }

        stage = SessionStage.next(stage)
    }

    fun getStage(): String {
        return stage
    }

    private fun setInactiveStage() {
        stage = SessionStage.INACTIVE
    }

    companion object {
        fun createSession(): Session {
            return Session(Random().nextInt(Int.MAX_VALUE))
        }

        fun createInactiveSession(): Session {
            val session = Session(Random().nextInt(Int.MAX_VALUE))
            session.setInactiveStage()
            return session
        }
    }
}

/**
 * Some metadata that needed to process packages correctly
 */
data class PackageMetadata(val port: Int, var additionalMetadata: ByteArray? = null) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as PackageMetadata

        if (port != other.port) return false
        if (!Arrays.equals(additionalMetadata, other.additionalMetadata)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = port
        result = 31 * result + (additionalMetadata?.let { Arrays.hashCode(it) } ?: 0)
        return result
    }
}

/**
 * Just a wrapper around InetAddress
 */
data class Address(val host: String, val port: Int) {
    fun toInetAddress(): InetAddress {
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
    companion object {
        val logger = loggerFor<Message>()
    }

    /**
     * Serializes payload of this message. Cause: Any serializes as ByteString (in my case for some reason), so we need to
     * manually serialize it to ByteArray and then manually deserialize in needed type.
     */
    fun serialize(): SerializedMessage {
        logger.info("Serializing payload $payload to bytes")

        val serializedPayload = if (payload == null)
            ByteArray(0)
        else
            SerializationUtils.anyToBytes(payload)

        return SerializedMessage(topic, type, serializedPayload)
    }
}

/**
 * Message with serialized payload. For inner usage only.
 */
data class SerializedMessage(val topic: String, val type: String, val payload: ByteArray) {
    companion object {
        val logger = loggerFor<SerializedMessage>()
    }

    /**
     * Deserializes payload in given class
     */
    fun <T> deserialize(_class: Class<T>): Message {
        logger.info("Deserializing payload $payload to type ${_class.canonicalName}")

        val deserializedPayload = SerializationUtils.bytesToAny(payload, _class)

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
        val logger = loggerFor<Package>()

        /**
         * Deserializes from bytes
         */
        fun deserialize(jsonBytes: ByteArray): Package? {
            return SerializationUtils.bytesToAny(jsonBytes)
        }

        /**
         * Serializes in bytes
         */
        fun serialize(pkg: Package): ByteArray? {
            return SerializationUtils.anyToBytes(pkg)
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