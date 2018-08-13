package net.stits.osen

import kotlinx.coroutines.experimental.launch
import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.ServerSocket
import java.net.Socket


/**
 * TODO:
 * Сделать подобие MQ. Сообщения приходят и кладутся в очередь. Когда сообщение приходит, запускается рабочий, который смотрит
 * пришедшие сообщения и смотрит список задач, которым нужно данное сообщение. Рабочий должен запускаться в контексе сессии,
 * чтобы была возможность отправить какой-то ответ назад.
 *
 * Список задач формируется:
 *  а) при вызове readMessage (одноразовая задача, надо как-то сделать корутиной)
 *  б) при наличии контроллера (многоразовая задача)
 *  в) при наличии флоу (многоразовая задача, пока не имплементить)
 * Рабочий выполняет задачу (сохраняет чего-нибудь, отправляет новые сообщения) и удаляет сообщение из очереди.
 */


typealias PackageProcessor = (pack: Package) -> Any?
typealias MessageTopic = String
typealias MessageType = String

object Flags {
    const val QUIT = 'q'
    const val MESSAGE = 'm'
    const val FLOW = 'f'
    const val RESPONSE = 'r'
}

// TODO: autoremove sessions
data class TCPSession(private val socket: Socket) {
    val output = DataOutputStream(socket.getOutputStream())
    val input = DataInputStream(socket.getInputStream())

    fun isClosed() = socket.isClosed
    fun close() = socket.close()
}

const val TCP_MAX_PACKAGE_SIZE_BYTES = 1024 * 10
const val TCP_TIMEOUT_SEC = 5

class TCP(private val port: Int) {
    companion object {
        val logger = loggerFor<TCP>()
    }

    private val beforeMessageSent = hashMapOf<MessageTopic, PackageModifier>()
    fun setBeforeMessageSent(topic: MessageTopic, modifier: PackageModifier) {
        beforeMessageSent[topic] = modifier
    }

    private val afterMessageSent = hashMapOf<MessageType, PackageModifier>()
    fun setAfterMessageSent(topic: MessageTopic, modifier: PackageModifier) {
        afterMessageSent[topic] = modifier
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
     */
    private fun createSession(peer: Address): TCPSession {
        val session = TCPSession(Socket(peer.host, peer.port))
        activeSessions[peer] = session

        return session
    }

    fun sessionPresents(peer: Address): Boolean {
        val cachedSession = activeSessions[peer]

        if (cachedSession != null && cachedSession.isClosed()) {
            removeSession(peer)
            return false
        }

        return cachedSession != null && !cachedSession.isClosed()
    }

    fun getSession(peer: Address): TCPSession? {
        return activeSessions[peer]
    }

    fun removeSession(peer: Address) {
        activeSessions.remove(peer)
    }

    private val clientSocket = Socket()

    fun listen(processPackage: PackageProcessor) {
        val serverSocket = ServerSocket(port)
        serverSocket.soTimeout = TCP_TIMEOUT_SEC
        logger.info("Listening for UDP packets on port: $port")

        try {
            val peerSocket = serverSocket.accept()
            val peer = Address(peerSocket.inetAddress.hostName, peerSocket.port)
            val session = createSession(peer, peerSocket)

            launch {
                while (!session.isClosed()) {
                    logger.info("Got connection from: $peer")

                    val (flag, pkg) = readPackage(session)

                    if (pkg == null) {
                        logger.warning("Closing connection...")
                        removeSession(peer)
                        session.close()
                        continue
                    }

                    logger.info("Read $pkg from $peer")

                    val afterPackageReceived = afterMessageSent[pkg.message.topic]
                    if (afterPackageReceived != null)
                        afterPackageReceived(pkg)

                    val response = processPackage(pkg)
                    logger.info("Processed package: $pkg, sending response: $response")

                    val topic = pkg.message.topic
                    val type = pkg.message.type
                    sendMessage(peer, Message(topic, type, response))
                }

                removeSession(peer)
            }
        } catch (e: Exception) {
            logger.warning(e.localizedMessage)
        }
    }

    private fun readPackage(session: TCPSession): Pair<Char, Package?> {
        val input = session.input

        val flag = input.readChar()

        when (flag) {
            Flags.QUIT -> {
                logger.info("Connection close flag received")
                return Pair(flag, null)
            }
            Flags.MESSAGE -> logger.info("Message flag received")
            Flags.FLOW -> logger.info("Flow flag received")
            Flags.RESPONSE -> logger.info("Response flag received")
        }

        val size = input.readInt()

        val serializedCompressedPackage = ByteArray(size)
        val receivedSize = input.read(serializedCompressedPackage)

        if (receivedSize != size) {
            logger.warning("Invalid package size received!")
            return Pair(Flags.QUIT, null)
        }

        val serializedPackage = CompressionUtils.decompress(serializedCompressedPackage)

        return Pair(flag, Package.deserialize(serializedPackage))
    }

    /**
     * Sends some message creating new session and waits until response for this session appears.
     * Triggers @OnRequest annotated method of controller.
     */
    fun <T> sendAndReceive(peer: Address, message: Message, _class: Class<T>): T? {
        val outPkg = sendMessage(peer, message)
        logger.info("Sent $outPkg to $peer, waiting for response...")

        val session = getSession(peer)!! // after sendMessage there should be a working session
        val inPkg = readPackage(session)

        if (inPkg == null) {
            logger.warning("Invalid response received")
            return null
        }

        val deserializedMessage = inPkg.message.deserialize(_class)

        if (deserializedMessage.payload == null) {
            logger.warning("Null payload received")
        }

        return deserializedMessage.payload
    }

    /**
     * This function is used for a stateless message exchange. It triggers @On annotated method of controller.
     */
    fun sendTo(peer: Address, messageBuilder: () -> Message) {
        val pkg = sendMessage(peer, messageBuilder())
        logger.info("Sent $pkg to $peer")
    }

    fun sendMessage(peer: Address, message: Message): Package {
        val serializedMessage = message.serialize()
        val metadata = PackageMetadata(port)
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

    private fun writePackage(pkg: Package, session: TCPSession) {
        val serializedPkg = Package.serialize(pkg)
                ?: throw IllegalArgumentException("Can not write empty package")

        val compressedAndSerializedPkg = CompressionUtils.compress(serializedPkg)

        if (TCP_MAX_PACKAGE_SIZE_BYTES < compressedAndSerializedPkg.size)
            throw RuntimeException("Unable to send packages with size more than $TCP_MAX_PACKAGE_SIZE_BYTES")

        session.output.write(compressedAndSerializedPkg.size)
        session.output.write(compressedAndSerializedPkg)
        session.output.flush()

        logger.info("Sending: $pkg")
    }
}