package net.stits.osen

import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.nio.aRead
import kotlinx.coroutines.experimental.nio.aWrite
import kotlinx.coroutines.experimental.runBlocking
import java.lang.reflect.Method
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel
import java.nio.charset.StandardCharsets
import java.util.*
import java.util.concurrent.TimeUnit


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

abstract class AbstractTCPSession(protected val channel: AsynchronousSocketChannel) {
    fun getAddress(): Address {
        val address = channel.remoteAddress as InetSocketAddress
        return Address(address.hostName, address.port)
    }

    fun isClosed() = !channel.isOpen
    fun close() = channel.close()
}

class TCPReadableSession(channel: AsynchronousSocketChannel) : AbstractTCPSession(channel) {
    private val buffer: ByteBuffer by lazy { ByteBuffer.allocate(TCP_MAX_PACKAGE_SIZE_BYTES) }

    companion object {
        private val logger = loggerFor<TCPReadableSession>()
    }

    fun read(): ByteArray = runBlocking {
        buffer.clear()
        logger.info("Reading $buffer")

        val size = channel.aRead(buffer, TCP_TIMEOUT_SEC, TCP_TIMEOUT_TIMEUNIT)
        val result = buffer.array().copyOfRange(0, size-1)

        logger.info("Closing channel...")
        close()

        result
    }
}

class TCPWritableSession(channel: AsynchronousSocketChannel) : AbstractTCPSession(channel) {
    companion object {
        private val logger = loggerFor<TCPWritableSession>()
    }

    fun write(data: ByteArray) = launch {
        val buffer = ByteBuffer.wrap(data)

        if (buffer.capacity() > TCP_MAX_PACKAGE_SIZE_BYTES) {
            logger.warning("Unable to send more than $TCP_MAX_PACKAGE_SIZE_BYTES bytes per request")
            return@launch
        }

        logger.info("Write $buffer begins...")
        channel.aWrite(buffer, TCP_TIMEOUT_SEC, TCP_TIMEOUT_TIMEUNIT)
        logger.info("Write $buffer complete!")

        logger.info("Closing channel...")
        close()
    }
}

const val TCP_MAX_PACKAGE_SIZE_BYTES = 1024 * 10
const val TCP_TIMEOUT_SEC = 5L
val TCP_TIMEOUT_TIMEUNIT = TimeUnit.SECONDS

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
    fun toInetSocketAddress() = InetSocketAddress(host, port)
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
    fun <T : Any> deserialize(_class: Class<T>): Message {
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