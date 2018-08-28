package net.stits.osen

import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.produce
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
    fun addBeforeMessageSent(topic: MessageTopic, modifier: PackageModifier) {
        beforeMessageSent[topic] = modifier
    }

    private val afterMessageReceived = hashMapOf<MessageType, PackageModifier>()
    fun addAfterMessageReceived(topic: MessageTopic, modifier: PackageModifier) {
        afterMessageReceived[topic] = modifier
    }

    private val portMappings = mutableListOf<Pair<Int, Int>>()
    private fun addPortMapping(readPort: Int, writePort: Int) {
        portMappings.add(Pair(readPort, writePort))
    }

    private fun getPortNym(port: Int): Int? {
        val portMapping = portMappings
                .find { (rPort, wPort) -> port == rPort || port == wPort }
                ?: return null

        return if (portMapping.first == port) portMapping.second else portMapping.first
    }

    private val activeSessions = hashMapOf<Address, TCPSession>()
    private fun storeSession(peer: Address, session: TCPSession) {
        activeSessions[peer] = session
    }
    private suspend fun createSession(peer: Address): TCPSession {
        val channel = AsynchronousSocketChannel.open()
        channel.aConnect(peer.toInetSocketAddress())

        return TCPSession(channel)
    }
    private fun getSessions() = activeSessions.entries
    private fun sessionPresents(peer: Address): Boolean {
        val cachedSession = getSession(peer)

        if (cachedSession != null && cachedSession.isClosed()) {
            removeSession(peer)
            return false
        }

        return cachedSession != null && !cachedSession.isClosed()
    }
    private fun getSession(peer: Address): TCPSession? {
        val portNym = getPortNym(peer.port)
                ?: return activeSessions[peer]

        val peerNym = Address(peer.host, portNym)

        return activeSessions[peer]
        ?: activeSessions[peerNym]
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
        for (session in sessions) launch {
            val pkg = readPackage(session)
            send(Pair(pkg, session))
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

    private suspend fun preProcessPackage(pkg: Package?, session: TCPSession, processPackage: PackageProcessor) {
        val peer = session.getAddress()

        if (pkg == null) {
            logger.warning("Closing connection...")
            return
        }

        logger.info("Read $pkg from $peer")

        val afterPackageReceived = afterMessageReceived[pkg.message.topic]
        if (afterPackageReceived != null)
            afterPackageReceived(pkg)

        val realPeerAddress = Address(peer.host, pkg.metadata.port)
        logger.info("$peer is actually listening on $realPeerAddress")
        addPortMapping(realPeerAddress.port, peer.port)

        val response = processPackage(pkg, realPeerAddress)
        logger.info("Processed package: $pkg")

        if (response != null) {
            logger.info("Sending response: $response")
            val topic = pkg.message.topic
            val type = pkg.message.type
            respondTo(peer, pkg.metadata.requestId!!, Message(topic, type, response))
        }
    }

    fun listen(processPackage: PackageProcessor) = launch {
        val address = InetSocketAddress(port)

        val serverChannel = AsynchronousServerSocketChannel.open().bind(address)
        logger.info("Listening for packets on port: $port")

        val sessions = sessions(serverChannel)
        for (session in sessions) {
            storeSession(session.getAddress(), session)
        }

        val packages = packages(sessions)
        for ((pkg, session) in packages) {
            preProcessPackage(pkg, session, processPackage)
        }
    }

    /**
     * This function is executed by @OnRequest annotated method with it's return value as a payload
     * It sends back response for a given session and triggers @OnResponse annotated method of controller.
     */
    private suspend fun respondTo(peer: Address, responseId: Long, message: Message) {
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

        val response = waitForResponse(requestId, TCP_TIMEOUT_SEC * 1000L)
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
    suspend fun sendTo(peer: Address, messageBuilder: () -> Message) {
        val pkg = sendMessage(peer, messageBuilder())
        logger.info("Sent $pkg to $peer")
    }

    private suspend fun sendMessage(peer: Address, message: Message, flag: Flag = Flags.MESSAGE, requestId: Long? = null): Package {
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

    private suspend fun writePackage(pkg: Package, session: TCPSession) {
        val serializedPkg = Package.serialize(pkg)
                ?: throw IllegalArgumentException("Can not write empty package")

        val compressedAndSerializedPkg = CompressionUtils.compress(serializedPkg)

        session.write(compressedAndSerializedPkg)

        logger.info("Sending: $pkg")
    }
}