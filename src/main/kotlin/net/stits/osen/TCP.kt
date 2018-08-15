package net.stits.osen

import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.withTimeoutOrNull
import java.net.ServerSocket
import java.net.Socket
import java.util.*
import kotlin.concurrent.thread


class TCP(private val port: Int) {
    companion object {
        val logger = loggerFor<TCP>()
    }

    private val beforeMessageSent = hashMapOf<MessageTopic, PackageModifier>()
    fun setBeforeMessageSent(topic: MessageTopic, modifier: PackageModifier) {
        beforeMessageSent[topic] = modifier
    }

    private val afterMessageReceived = hashMapOf<MessageType, PackageModifier>()
    fun setAfterMessageReceived(topic: MessageTopic, modifier: PackageModifier) {
        afterMessageReceived[topic] = modifier
    }

    private val activeSessions = hashMapOf<Address, TCPSession>()
    /**
     * Starts session from existing connection
     */
    private fun createSession(peer: Address, socket: Socket): TCPSession {
        val session = TCPSession(socket)
        activeSessions[peer] = session

        return session
    }

    /**
     * Creates new connection and starts session
     *
     * TODO: проверять, если у меня уже есть сессия хотя бы с одним сокетом (настоящий и рандомный)
     */
    private fun createSession(peer: Address): TCPSession {
        val session = TCPSession(Socket(peer.host, peer.port))
        activeSessions[peer] = session

        return session
    }

    private fun sessionPresents(peer: Address): Boolean {
        val cachedSession = activeSessions[peer]

        if (cachedSession != null && cachedSession.isClosed()) {
            removeSession(peer)
            return false
        }

        return cachedSession != null && !cachedSession.isClosed()
    }

    private fun getSession(peer: Address): TCPSession? {
        return activeSessions[peer]
    }

    private fun removeSession(peer: Address) {
        activeSessions.remove(peer)
    }

    fun listen(processPackage: PackageProcessor) {
        val serverSocket = ServerSocket(port)
        serverSocket.soTimeout = TCP_TIMEOUT_SEC * 1000
        logger.info("Listening for UDP packets on port: $port")

        thread {
            while (true) {
                val peerSocket = serverSocket.accept()
                val peer = Address(peerSocket.inetAddress.hostName, peerSocket.port)
                val session = createSession(peer, peerSocket)

                launch {
                    while (!session.isClosed()) {
                        logger.info("Got connection from: $peer")

                        val pkg = readPackage(session)

                        if (pkg == null) {
                            logger.warning("Closing connection...")
                            removeSession(peer)
                            session.close()
                            continue
                        }

                        logger.info("Read $pkg from $peer")

                        val afterPackageReceived = afterMessageReceived[pkg.message.topic]
                        if (afterPackageReceived != null)
                            afterPackageReceived(pkg)

                        val response = processPackage(pkg, peer)
                        logger.info("Processed package: $pkg")

                        if (response != null) {
                            logger.info("Sending response: $response")
                            val topic = pkg.message.topic
                            val type = pkg.message.type
                            respondTo(peer, pkg.metadata.requestId!!, Message(topic, type, response))
                        }
                    }

                    removeSession(peer)
                }
            }
        }
    }

    /**
     * This function is executed by @OnRequest annotated method with it's return value as a payload
     * It sends back response for a given session and triggers @OnResponse annotated method of controller.
     */
    private fun respondTo(peer: Address, responseId: Long, message: Message) {
        val pkg = sendMessage(peer, message, Flags.RESPONSE, responseId)
        logger.info("Responded $pkg to $peer with responseId: $responseId")
    }

    /**
     * Sends some message creating new session and waits until response for this session appears.
     * Triggers @OnRequest annotated method of controller.
     */
    suspend fun <T> sendAndReceive(peer: Address, message: Message, _class: Class<T>): T? {
        val requestId = createRequest(_class)
        val outPkg = sendMessage(peer, message, Flags.REQUEST, requestId)
        logger.info("Sent $outPkg to $peer, waiting for response...")

        val response = waitForResponse(requestId, TCP_TIMEOUT_SEC.toLong())
        removeRequest(requestId)

        return _class.cast(response)
    }

    private fun removeRequest(id: Long) {
        responses.remove(id)
    }

    private fun <T> createRequest(_class: Class<T>): Long {
        val requestId = Random().nextLong()
        val newResponse = TCPResponse(null, _class)

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

    fun addResponse(responseId: Long, payload: ByteArray?) {
        if (!responses.containsKey(responseId)) {
            logger.warning("Unknown response with id: $responseId")
            return
        }

        responses[responseId]!!.payload = payload
    }

    /**
     * Watches responses map for specific responseId until response appears or timeout is passed
     */
    private suspend fun waitForResponse(responseId: Long, timeout: Long) = withTimeoutOrNull(timeout) {
        val delay = 5
        repeat(timeout.div(delay).toInt()) {
            if (!responses.containsKey(responseId)) {
                logger.warning("Unknown request with id: $responseId")
                return@withTimeoutOrNull null
            }

            val response = responses[responseId]

            if (response!!.payload != null) {
                logger.info("Received response: $response for request: $responseId")
                return@withTimeoutOrNull response
            }

            kotlinx.coroutines.experimental.delay(delay)
        }
    }

    /**
     * This function is used for a stateless message exchange. It triggers @On annotated method of controller.
     */
    fun sendTo(peer: Address, messageBuilder: () -> Message) {
        val pkg = sendMessage(peer, messageBuilder())
        logger.info("Sent $pkg to $peer")
    }

    private fun sendMessage(peer: Address, message: Message, flag: Flag = Flags.MESSAGE, requestId: Long? = null): Package {
        val serializedMessage = message.serialize()
        val metadata = PackageMetadata(port, flag, requestId)
        val pkg = Package(serializedMessage, metadata)

        val beforePackageSent = beforeMessageSent[message.topic]
        if (beforePackageSent != null)
            beforePackageSent(pkg)

        val session = if (sessionPresents(peer))
            getSession(peer)!!
        else
            createSession(peer)

        writePackage(pkg, session)

        return pkg
    }

    private fun readPackage(session: TCPSession): Package? {
        val input = session.input

        // TODO: maybe make size long?
        val size = input.readInt()

        val serializedCompressedPackage = ByteArray(size)
        val receivedSize = input.read(serializedCompressedPackage)

        if (receivedSize != size) {
            logger.warning("Invalid package size received!")
            return null
        }

        val serializedPackage = CompressionUtils.decompress(serializedCompressedPackage)

        return Package.deserialize(serializedPackage)
    }

    private fun writePackage(pkg: Package, session: TCPSession) {
        val serializedPkg = Package.serialize(pkg)
                ?: throw IllegalArgumentException("Can not write empty package")

        val compressedAndSerializedPkg = CompressionUtils.compress(serializedPkg)

        if (TCP_MAX_PACKAGE_SIZE_BYTES < compressedAndSerializedPkg.size)
            throw RuntimeException("Unable to send packages with size more than $TCP_MAX_PACKAGE_SIZE_BYTES")

        session.output.writeInt(compressedAndSerializedPkg.size)
        session.output.write(compressedAndSerializedPkg)

        logger.info("Sending: $pkg")
    }
}