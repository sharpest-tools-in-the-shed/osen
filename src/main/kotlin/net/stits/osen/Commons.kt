package net.stits.osen

import java.io.DataInputStream
import java.io.DataOutputStream
import java.lang.reflect.Method
import java.net.InetAddress
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.util.*


typealias Flag = Char
object Flags {
    const val REQUEST: Flag = 'r'
    const val MESSAGE: Flag = 'm'
    const val RESPONSE: Flag = 's'
}


/**
 * Annotation that is used to mark controller classes
 *
 * @param topic {String} Message Topic for which this controller is responsible
 */
@Target(AnnotationTarget.CLASS)
annotation class P2PController(val topic: String)

/**
 * Annotation that is used to mark methods of controller class that should handle some type of messages
 *
 * @param type {String} when we receive message with this type, this method invocation is triggered
 */
@Target(AnnotationTarget.FUNCTION)
annotation class On(val type: String)

typealias PackageProcessor = (pack: Package, peer: Address) -> Any?
typealias MessageTopic = String
typealias MessageType = String

data class TCPSession(private val socket: Socket) {
    val output = DataOutputStream(socket.getOutputStream())
    val input = DataInputStream(socket.getInputStream())

    fun isClosed() = socket.isClosed
    fun close() = socket.close()
}

const val TCP_MAX_PACKAGE_SIZE_BYTES = 1024 * 10
const val TCP_TIMEOUT_SEC = 5

data class TCPResponse<T>(var payload: ByteArray?, val _class: Class<T>) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as TCPResponse<*>

        if (!Arrays.equals(payload, other.payload)) return false
        if (_class != other._class) return false

        return true
    }

    override fun hashCode(): Int {
        var result = payload?.let { Arrays.hashCode(it) } ?: 0
        result = 31 * result + _class.hashCode()
        return result
    }
}

/**
 * Mapping [Message topic -> Controller that handles this topic]
 */
typealias TopicHandlers = HashMap<String, TopicController>

/**
 * Type of callbacks that invokes at different lifecycle steps
 */
typealias PackageModifier = (pack: Package) -> Unit

/**
 * Object containing controller and mapping [Message type -> Method that handles this message type]
 * Only one handler per unique together Topic and Type is possible right now.
 *
 * TODO: maybe switch to concurrent hash map
 * TODO: maybe add support for multiple handlers per topic-type
 *
 * @param controller {Any} @P2PController-annotated instance
 * @param onListeners {String -> Method} mapping [MessageType -> @On annotated method]
 * @param onRequestListeners {String -> Method} mapping [MessageType -> @OnRequest annotated method]
 * @param onResponseListeners {String -> Method} mapping [MessageType -> @OnResponse annotated method]
 */
data class TopicController(
        val controller: Any,
        val onListeners: Map<String, Method>
)

/**
 * Some metadata that needed to process packages correctly
 */
data class PackageMetadata(val port: Int, val flag: Flag, val requestId: Long? = null)

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
data class Message(val topic: MessageTopic, val type: MessageType, val payload: Any? = null) {
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
data class SerializedMessage(val topic: MessageTopic, val type: MessageType, val payload: ByteArray) {
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