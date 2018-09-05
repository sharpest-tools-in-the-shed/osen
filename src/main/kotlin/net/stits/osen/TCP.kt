package net.stits.osen

import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.channels.consumeEach
import kotlinx.coroutines.experimental.channels.produce
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.nio.aAccept
import kotlinx.coroutines.experimental.nio.aConnect
import kotlinx.coroutines.experimental.withTimeoutOrNull
import java.net.InetSocketAddress
import java.nio.channels.AsynchronousServerSocketChannel
import java.nio.channels.AsynchronousSocketChannel
import java.util.*


class TCP(private val port: Int) {
    companion object {
        val logger = loggerFor<TCP>()
    }

    private val beforeMessageSent = hashMapOf<MessageTopic, PackageModifier>()
    fun putBeforeMessageSent(topic: MessageTopic, modifier: PackageModifier) {
        beforeMessageSent[topic] = modifier
    }

    private val afterMessageReceived = hashMapOf<MessageType, PackageModifier>()
    fun putAfterMessageReceived(topic: MessageTopic, modifier: PackageModifier) {
        afterMessageReceived[topic] = modifier
    }

    private fun getReadableSessions(serverChannel: AsynchronousServerSocketChannel) =
        produce(capacity = Channel.UNLIMITED) {
            while (true) {
                val channel = serverChannel.aAccept()
                send(TCPReadableSession(channel))
            }
        }

    /**
     * So, i have 2 ways where i can get a new session:
     *  1. from listen
     *  2. from send
     * In both ways i should:
     *  - check if i already have session with this peer
     *  - add it to producer
     *  - on new package receive add peer address from package to peer mapping
     *
     *             channels
     *             /------\
     *  write ------------------ read
     *             \------/
     */

    private fun preprocessPackage(pkg: Package?, peer: Address, processPackage: PackageProcessor) = launch {
        logger.info("Got connection from peer: $peer")

        if (pkg == null) {
            logger.warning("Closing connection with peer: $peer...")

            return@launch
        }

        val afterPackageReceived = afterMessageReceived[pkg.message.topic]
        if (afterPackageReceived != null)
            afterPackageReceived(pkg)

        val realPeerAddress = Address(peer.host, pkg.metadata.port)
        logger.info("$peer is actually listening on $realPeerAddress")

        val response = processPackage(pkg, realPeerAddress)
        logger.info("Processed package: $pkg")

        if (response != null) {
            logger.info("Sending response: $response")
            val topic = pkg.message.topic
            val type = pkg.message.type
            respondTo(realPeerAddress, pkg.metadata.requestId!!, Message(topic, type, response))
        }
    }

    private fun createServerSocketChannel(address: InetSocketAddress) =
        AsynchronousServerSocketChannel.open().bind(address)

    fun listen(processPackage: PackageProcessor) = launch {
        val address = InetSocketAddress(port)

        val serverChannel = createServerSocketChannel(address)
        logger.info("Listening for packets on port: $port")

        val sessions = getReadableSessions(serverChannel)
        sessions.consumeEach { session ->
            val peer = session.getAddress()
            val pkg = readPackage(session)
            preprocessPackage(pkg, peer, processPackage)
        }
    }

    /**
     * This function is executed by @OnRequest annotated method with it's return value as a payload
     * It sends back response for a given session and triggers @OnResponse annotated method of controller.
     */
    private suspend fun respondTo(peer: Address, responseId: Long, message: Message) {
        val pkg = sendMessage(peer, message, Flags.RESPONSE, responseId)
        logger.info("Responded with package: $pkg to peer: $peer with responseId: $responseId")
    }

    /**
     * Sends some message creating new session and waits until response for this session appears.
     * Triggers @OnRequest annotated method of controller.
     */
    suspend fun <T : Any> sendAndReceive(peer: Address, message: Message, clazz: Class<T>): T {
        val requestId = createRequest(clazz)
        val outPkg = sendMessage(peer, message, Flags.REQUEST, requestId)
        logger.info("Sent package: $outPkg to peer: $peer, waiting for response...")

        val response = waitForResponse(requestId, TCP_TIMEOUT_SEC * 1000)
        removeRequest(requestId)
        logger.info("Received response: $response from peer: $peer")

        val payload = response?.payload
            ?: throw RuntimeException("Unable to get response of type: ${clazz.canonicalName} from peer: $peer")

        return SerializationUtils.bytesToAny(payload, clazz)
    }

    private fun removeRequest(id: Long) {
        responses.remove(id)
    }

    private fun <T> createRequest(clazz: Class<T>): Long {
        val requestId = Random().nextLong()
        val newResponse = TCPResponse(null, clazz)

        responses[requestId] = newResponse

        return requestId
    }

    /**
     * This map is used to store responses. After sendAndReceive() with specified invoked it watch
     * this map for a response (request id is used as key) and returns it when response arrives.
     *
     * TODO: maybe make it concurrent
     */
    private val responses = hashMapOf<Long, TCPResponse<*>>()

    fun addResponse(responseId: Long, payload: ByteArray) {
        if (!responses.containsKey(responseId)) {
            logger.warning("Unknown response with id: $responseId")
            return
        }

        responses[responseId]!!.payload = payload
    }

    /**
     * Watches responses map for specific responseId until response appears or timeout is passed
     */
    private suspend fun waitForResponse(responseId: Long, timeout: Long): TCPResponse<*>? = withTimeoutOrNull(timeout) {
        val delayMs = 1
        var result: TCPResponse<*>? = null

        repeat(timeout.div(delayMs).toInt()) {
            if (!responses.containsKey(responseId)) {
                logger.warning("Unknown request with id: $responseId")
                result = null
            }

            val response = responses[responseId]

            if (response!!.payload != null) {
                logger.info("Received response: $response for request: $responseId")
                result = response
                return@withTimeoutOrNull result
            }

            delay(delayMs)
        }

        return@withTimeoutOrNull result
    }

    /**
     * This function is used for a stateless message exchange. It triggers @On annotated method of controller.
     */
    suspend fun sendTo(peer: Address, message: Message) {
        val pkg = sendMessage(peer, message)
        logger.info("Sent package: $pkg to peer: $peer")
    }

    private suspend fun createSocketChannel(peer: Address): AsynchronousSocketChannel {
        val channel = AsynchronousSocketChannel.open()
        channel.aConnect(peer.toInetSocketAddress())
        logger.info("Connected to peer: $peer")

        return channel
    }

    private suspend fun sendMessage(
        peer: Address,
        message: Message,
        flag: Flag = Flags.MESSAGE,
        requestId: Long? = null
    ): Package {
        logger.info("Sending message: $message to peer: $peer")

        val serializedMessage = message.serialize()
        val metadata = PackageMetadata(port, flag, requestId)
        val pkg = Package(serializedMessage, metadata)

        val beforePackageSent = beforeMessageSent[message.topic]
        if (beforePackageSent != null)
            beforePackageSent(pkg)

        val channel = createSocketChannel(peer)
        val session = TCPWritableSession(channel)

        writePackage(pkg, session)

        return pkg
    }

    private fun readPackage(session: TCPReadableSession): Package? {
        val serializedAndCompressedPackage = session.read()

        val serializedPackage = CompressionUtils.decompress(serializedAndCompressedPackage)

        return Package.deserialize(serializedPackage)
    }

    private fun writePackage(pkg: Package, session: TCPWritableSession) {
        logger.info("Sending: $pkg")

        val serializedPkg = Package.serialize(pkg)
            ?: throw IllegalArgumentException("Can not write empty package")

        val compressedAndSerializedPkg = CompressionUtils.compress(serializedPkg)

        val job = session.write(compressedAndSerializedPkg)
        while (!job.isCancelled && !job.isCompleted) { /* why? */
        }
    }
}