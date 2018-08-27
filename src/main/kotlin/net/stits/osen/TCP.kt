package net.stits.osen

import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.produce
import kotlinx.coroutines.experimental.nio.aAccept
import kotlinx.coroutines.experimental.runBlocking
import kotlinx.coroutines.experimental.withTimeoutOrNull
import java.net.InetSocketAddress
import java.net.Socket
import java.nio.channels.AsynchronousServerSocketChannel
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
    private fun createSession(socket: Socket) = TCPSession(socket)

    private fun storeSession(peer: Address, session: TCPSession) {
        activeSessions[peer] = session
    }

    private fun getSessions() = activeSessions.entries

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

    private fun sessions(serverChannel: AsynchronousServerSocketChannel) = produce {
        while (true) {
            val channel = serverChannel.aAccept()
            send(TCPSession(channel))
        }
    }

    private fun packages(sessions: ReceiveChannel<TCPSession>) = produce {
        for (session in sessions) {
            val pkg = readPackage(session)
            send(pkg)
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

    fun listen(processPackage: PackageProcessor) = runBlocking {
        val address = InetSocketAddress(port)

        val serverChannel = AsynchronousServerSocketChannel.open().bind(address)
        logger.info("Listening for packets on port: $port")

        val sessions = sessions(serverChannel)
        for (session in sessions) {
            storeSession(session.getAddress(), session)
        }

        val packages = packages(sessions)
        for (pkg in packages) {

        }

        thread {
            while (true) {
                getSessions().forEach { (peer, session) ->
                    if (!session.isClosed()) {
                        logger.info("Got connection from: $peer")

                        val pkg = readPackage(session)

                        if (pkg == null) {
                            logger.warning("Closing connection...")
                            removeSession(peer)
                            session.close()
                            return@forEach
                        }

                        logger.info("Read $pkg from $peer")

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
        else {
            val session = createSession(peer)
            storeSession(peer, session)
            session
        }

        writePackage(pkg, session)

        return pkg
    }

    private suspend fun readPackage(session: TCPSession): Package? {
        val serializedAndCompressedPackage = session.read()

        val serializedPackage = CompressionUtils.decompress(serializedAndCompressedPackage)

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