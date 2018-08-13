package net.stits.osen

import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.withTimeoutOrNull
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.config.BeanDefinition
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider
import org.springframework.context.support.GenericApplicationContext
import org.springframework.core.type.filter.AnnotationTypeFilter
import java.lang.reflect.Method
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.Socket
import java.nio.charset.StandardCharsets
import javax.annotation.PostConstruct
import kotlin.concurrent.thread

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
        val onListeners: Map<String, Method>,
        val onRequestListeners: Map<String, Method>,
        val onResponseListeners: Map<String, Method>
)

/**
 * TODO: maybe implement flows in TCP? it would be nice to write flows like methods of controller (absolutely sequentially)
 * TODO: split this class into 2: first handles all annotation stuff, second handles networking
 * TODO: add annotation @SpringP2PApplication(port) that starts network when annotating some class - this is not possible (or you need to change spring initialization procedure)
 * TODO: switch request-response to TCP
 *
 * On construction it scans all packages (or package you pass to it) and finds classes annotated with @P2PController,
 * adds them to Spring Context, then it finds all methods of this class with annotations and saves this information.
 * Then it starts UDP-server on specified port and listens for packets.
 * Every packet is transformed (bytes -> json) into net.stits.osen.Package object that contains net.stits.osen.Message
 * object that contains Topic, Type and Session which are used to determine what controller method should be invoked.
 *
 * @param basePackages {Array<String>} - you should specify this in order to say Spring classpath scanner where do
 * your @P2PControllers placed // TODO: somehow get this values from spring
 * @param maxPacketSizeBytes {Int} - you can't send and receive packages larger than this value
 */
class P2P(private val basePackages: Array<String>, private val maxPacketSizeBytes: Int = MAX_PACKET_SIZE_BYTES) {
    private val topicHandlers: TopicHandlers = hashMapOf()

    private val clientSocketUDP = DatagramSocket()
    private val clientSocketTCP = Socket()

    private var afterPackageReceived: PackageModifier? = null
    private var beforePackageSent: PackageModifier? = null

    @Autowired
    lateinit var context: GenericApplicationContext

    var listeningPort: Int = 1337

    /**
     * This map is used to store responses. After requestFrom() or requestFromTimeouted() with specified invoked it watch
     * this map for a response (session id is used as key) and returns it when response arrives.
     *
     * TODO: maybe make it concurrent
     */
    val responses = hashMapOf<Int, Any?>()

    /**
     * This method is invoked by Spring. It scans classpath and then initializes network
     */
    @PostConstruct
    private fun init() {
        scanClasspathAndAddP2PControllers()

        // configuring port according to node.port if specified
        val portFromProps = context.environment.getProperty("node.port")
        if (portFromProps != null) {
            logger.info("Found node.port spring property")
            listeningPort = portFromProps.toInt()
        }

        logger.info("Spring P2P extension successfully initialized, starting network up...")

        thread {
            initUDP()
        }
    }

    /**
     * This method scans through specified packages, finds all needed classes and methods
     */
    private fun scanClasspathAndAddP2PControllers() {
        val provider = ClassPathScanningCandidateComponentProvider(false)
        provider.addIncludeFilter(AnnotationTypeFilter(P2PController::class.java))

        val beanDefinitions = mutableListOf<BeanDefinition>()
        basePackages.forEach { pack -> beanDefinitions.addAll(provider.findCandidateComponents(pack)) }
        beanDefinitions.forEach { beanDefinition ->

            // adding found @P2PControllers to spring context
            val beanClass = Class.forName(beanDefinition.beanClassName)
            context.registerBean(beanClass)

            // getting topic name
            val messageTopic = beanClass.getAnnotation(P2PController::class.java).topic
            logger.info("Found P2P controller: ${beanClass.canonicalName} (topic: $messageTopic)")

            // finding annotated methods
            val onListeners = getAllAnnotatedOnMethodsOfBean(beanClass)
            val onRequestListeners = getAllAnnotatedOnRequestMethodsOfBean(beanClass)
            val onResponseListeners = getAllAnnotatedOnResponseMethodsOfBean(beanClass)

            // adding instances of @P2PControllers to list of topic handlers
            val beanInstance = context.getBean(beanClass)
            val topicController = TopicController(beanInstance, onListeners, onRequestListeners, onResponseListeners)
            topicHandlers[messageTopic] = topicController
        }
    }

    /**
     * This method is used to check there are no unexpected arguments on controller methods and throw exception at startup
     */
    private fun assertArgumentsPresentAt(_class: Class<*>, method: Method, signatureBuilder: () -> String) {
        var payloadFound = false
        var addressFound = false

        assert(method.parameters.size <= 2) {
            "Method ${method.name} of class $_class has more than 2 arguments.\n${signatureBuilder()}"
        }

        assert(
                method.parameters
                        .map { parameter ->
                            when {
                                Address::class.java.isAssignableFrom(parameter.type) && !addressFound -> {
                                    addressFound = true
                                    true
                                }
                                Address::class.java.isAssignableFrom(parameter.type) && addressFound && !payloadFound -> {
                                    payloadFound = true
                                    true
                                }
                                !payloadFound -> {
                                    payloadFound = true
                                    true
                                }
                                else -> false
                            }
                        }
                        .all { valid -> valid }

        ) { "Method ${method.name} of class $_class has an invalid signature.\n${signatureBuilder()}" }
    }

    /**
     * This method checks if given method has return value and throws at startup if there is no one
     */
    private inline fun assertReturnValuePresentAt(_class: Class<*>, method: Method, signatureBuilder: () -> String) {
        assert(method.returnType != Void.TYPE) {
            "Method ${method.name} of class $_class has no return value.\n${signatureBuilder()}"
        }
    }

    /**
     * This method checks if given method doesn't have return value and throws at startup if there is one
     */
    private inline fun assertReturnValueNotPresentAt(_class: Class<*>, method: Method, signatureBuilder: () -> String) {
        assert(method.returnType == Void.TYPE) {
            "Method ${method.name} of class $_class has no return value.\n${signatureBuilder()}"
        }
    }

    /**
     * Finds all methods of a given class which have @On annotation
     *
     * --- wish someday kotlin will support annotation inheritance =C ---
     */
    private fun getAllAnnotatedOnMethodsOfBean(beanClass: Class<*>): Map<String, Method> {
        val onMethods = beanClass.methods.filter { method -> method.isAnnotationPresent(On::class.java) }
        val listeners = hashMapOf<String, Method>()

        onMethods.forEach { method ->
            val messageType = method.getAnnotation(On::class.java).type
            val methodArgs = method.parameters.map { "${it.name}:${it.type}" }

            logger.info("\tFound @On annotated method: ${method.name} (type: $messageType)")
            if (methodArgs.isNotEmpty())
                logger.info("\t - Parameters: ${methodArgs.joinToString(", ")}")

            val signatureError = "@On annotated method signature is (a: <Any payload type>, b: Address) -> Unit"
            assertArgumentsPresentAt(beanClass, method) { signatureError }
            assertReturnValueNotPresentAt(beanClass, method) { signatureError }

            listeners[messageType] = method
        }

        return listeners
    }

    /**
     * Finds all methods of a given class which have @OnRequest annotation
     *
     * --- wish someday kotlin will support annotation inheritance =C ---
     */
    private fun getAllAnnotatedOnRequestMethodsOfBean(beanClass: Class<*>): Map<String, Method> {
        val onMethods = beanClass.methods.filter { method -> method.isAnnotationPresent(OnRequest::class.java) }
        val listeners = hashMapOf<String, Method>()

        onMethods.forEach { method ->
            val messageType = method.getAnnotation(OnRequest::class.java).type
            val methodArgs = method.parameters.map { "${it.name}:${it.type}" }

            logger.info("\tFound @OnRequest annotated method: ${method.name} (type: $messageType)")
            if (methodArgs.isNotEmpty())
                logger.info("\t - Parameters: ${methodArgs.joinToString(", ")}")

            val signatureError = "@OnRequest annotated method signature is (a: <Any request payload type>, b: Address) -> <Any response payload type>"
            assertArgumentsPresentAt(beanClass, method) { signatureError }
            assertReturnValuePresentAt(beanClass, method) { signatureError }

            listeners[messageType] = method
        }

        return listeners
    }

    /**
     * Finds all methods of a given class which have @OnResponse annotation
     *
     * --- wish someday kotlin will support annotation inheritance =C ---
     */
    private fun getAllAnnotatedOnResponseMethodsOfBean(beanClass: Class<*>): Map<String, Method> {
        val onMethods = beanClass.methods.filter { method -> method.isAnnotationPresent(OnResponse::class.java) }
        val listeners = hashMapOf<String, Method>()

        onMethods.forEach { method ->
            val messageType = method.getAnnotation(OnResponse::class.java).type
            val methodArgs = method.parameters.map { "${it.name}:${it.type}" }

            logger.info("\tFound @OnResponse annotated method: ${method.name} (type: $messageType)")
            if (methodArgs.isNotEmpty())
                logger.info("\t - Parameters: ${methodArgs.joinToString(", ")}")

            val signatureError = "@OnResponse annotated method signature is (a: <Any response payload type>, b: Address) -> <Any return value type>"
            assertArgumentsPresentAt(beanClass, method) { signatureError }
            assertReturnValuePresentAt(beanClass, method) { signatureError }

            listeners[messageType] = method
        }

        return listeners
    }

    /**
     * Initializes network (listens for packets and executes controller methods).
     *
     * Usage cheatsheet:
     * [node 1]: sendTo() -> [node 2]: @On
     * [node 1]: requestFrom() -> [node 2]: @OnRequest -> [node 1]: @OnResponse
     */
    private fun initUDP() {
        val serverSocket = DatagramSocket(listeningPort)
        val packet = DatagramPacket(ByteArray(maxPacketSizeBytes), maxPacketSizeBytes)

        logger.info("Listening for UDP packets on port: $listeningPort")

        while (true) {
            serverSocket.receive(packet)

            val sender = Address(packet.address.hostAddress, packet.port)
            logger.info("Got connection from: $sender")

            val pkg = readPackage(packet)

            if (pkg == null) {
                logger.warning("Received an empty package")
                continue
            }

            logger.info("Read $pkg from $sender")

            // here MITM can be prevented
            if (afterPackageReceived != null)
                afterPackageReceived!!(pkg)

            val actualSender = Address(sender.host, pkg.metadata.port)
            logger.info("$sender is actually $actualSender")

            val topic = pkg.message.topic
            val type = pkg.message.type
            val session = pkg.metadata.session
            val topicHandler = topicHandlers[topic]

            if (topicHandler == null) {
                logger.warning("No controller or flow for topic $topic, skipping...")
                continue
            }

            when (session.getStage()) {
                SessionStage.REQUEST -> handleOnRequestInvocation(topicHandler, topic, type, pkg.message, actualSender, session)
                SessionStage.RESPONSE -> handleOnResponseInvocation(topicHandler, topic, type, pkg.message, actualSender, session)
                SessionStage.INACTIVE -> handleOnInvocation(topicHandler, topic, type, pkg.message, actualSender)
                else -> logger.warning("Session ${session.id} is expired. Won't invoke anything.")
            }
        }
    }

    /**
     * Finds needed @On annotated method and executes it with correct arguments
     *
     * TODO: somehow get rid of duplication
     */
    private fun handleOnInvocation(topicHandler: TopicController, topic: String, type: String, message: SerializedMessage, recipient: Address) {
        val messageHandler = topicHandler.onListeners[type]
        if (messageHandler == null) {
            logger.warning("No @On annotated method to handle message of type: $type of topic: $topic, skipping...")
            return
        }

        val arguments = parseMethodArguments(messageHandler, recipient, message)

        logger.info("Trying to invoke method: ${messageHandler.name} with arguments: ${arguments.toList()}")
        launch { messageHandler.invoke(topicHandler.controller, *arguments) }
    }

    /**
     * Finds needed @OnRequest annotated method and executes it with correct arguments
     *
     * TODO: somehow get rid of duplication
     */
    private fun handleOnRequestInvocation(topicHandler: TopicController, topic: String, type: String, requestMessage: SerializedMessage, recipient: Address, session: Session) {
        val messageHandler = topicHandler.onRequestListeners[type]
        if (messageHandler == null) {
            logger.warning("No @OnRequest annotated method to handle message of type: $type of topic: $topic, skipping...")
            return
        }

        val arguments = parseMethodArguments(messageHandler, recipient, requestMessage)

        logger.info("Trying to invoke method: ${messageHandler.name} with arguments: ${arguments.toList()}")
        launch {
            val payload = messageHandler.invoke(topicHandler.controller, *arguments)
            val responseMessage = Message(topic, type, payload)
            respondTo(recipient, session, responseMessage)
        }
    }

    /**
     * Finds needed @OnResponse annotated method and executes it with correct arguments
     *
     * TODO: somehow get rid of duplication
     */
    private fun handleOnResponseInvocation(topicHandler: TopicController, topic: String, type: String, responseMessage: SerializedMessage, recipient: Address, session: Session) {
        val messageHandler = topicHandler.onResponseListeners[type]
        if (messageHandler == null) {
            logger.warning("No @OnResponse method to handle message of type: $type of topic: $topic, skipping...")
            return
        }

        val arguments = parseMethodArguments(messageHandler, recipient, responseMessage)

        logger.info("Trying to invoke method: ${messageHandler.name} with arguments: ${arguments.toList()}")
        launch { responses[session.id] = messageHandler.invoke(topicHandler.controller, *arguments) } // TODO: possible memory leak (it can be stored after it deleted in requestFrom())
    }

    /**
     * Maps method parameters to arguments
     */
    private fun parseMethodArguments(method: Method, recipient: Address, message: SerializedMessage): Array<Any?> {
        return Array(method.parameters.size) { index ->
            val parameter = method.parameters[index]

            when {
                Address::class.java.isAssignableFrom(parameter.type) -> recipient
                message.payload.isEmpty() -> null
                else -> {
                    message.deserialize(parameter.type).payload
                            ?: error("Payload (${message.payload.toString(StandardCharsets.UTF_8)}) deserialization failure, skipping...")
                }
            }
        }
    }

    companion object {
        val logger = loggerFor<P2P>()
    }

    /**
     * This function is used for a stateless message exchange. It triggers @On annotated method of controller.
     */
    fun sendTo(recipient: Address, messageBuilder: () -> Message) {
        val session = Session.createInactiveSession()
        val pkg = sendMessageUDP(recipient, messageBuilder(), listeningPort, session)
        logger.info("Sent $pkg to $recipient with inactive session")
    }

    /**
     * This functions are just shorthands for sendAndReceive()
     */
    suspend inline fun <reified T : Any> requestFrom(recipient: Address, messageBuilder: () -> Message): T? = sendAndReceive(recipient, messageBuilder(), T::class.java, 5000)

    suspend inline fun <reified T : Any> requestFromTimeouted(recipient: Address, timeout: Long, messageBuilder: () -> Message): T? = sendAndReceive(recipient, messageBuilder(), T::class.java, timeout)

    /**
     * This function is executed by @OnRequest annotated method with it's return value as a payload
     * It sends back response for a given session and triggers @OnResponse annotated method of controller.
     */
    private fun respondToUDP(recipient: Address, session: Session, message: Message) {
        when (session.getStage()) {
            SessionStage.REQUEST -> session.processLifecycle()
            else -> throw IllegalArgumentException("Session ${session.id} is in illegal stage.")
        }

        val pkg = sendMessageUDP(recipient, message, listeningPort, session)
        logger.info("Responded $pkg to $recipient with session: $session")
    }

    /**
     * Sends some message creating new session and waits until response for this session appears.
     * Triggers @OnRequest annotated method of controller.
     */
    suspend fun <T> sendAndReceiveUDP(recipient: Address, message: Message, _class: Class<T>, timeout: Long = 30000): T? {
        val session = Session.createSession()

        val pkg = sendMessageUDP(recipient, message, listeningPort, session)
        logger.info("Sent $pkg to $recipient, waiting for response...")

        val response = waitForResponse(session.id, timeout)
        responses.remove(session.id)

        return _class.cast(response)
    }

    /**
     * Watches responses map for specific responseId until response appears or timeout is passed
     */
    private suspend fun waitForResponse(responseId: Int, timeout: Long) = withTimeoutOrNull(timeout) {
        val delay = 5
        repeat(timeout.div(delay).toInt()) {
            if (responses.containsKey(responseId)) {
                val response = responses[responseId]

                logger.info("Received response: $response")
                return@withTimeoutOrNull response
            } else {
                delay(delay)
            }
        }
    }

    /**
     * Same as sendTo() or requestFrom() but u can set all parameters by yourself
     * This function can be used to implement some non-standard logic
     */
    fun sendMessageUDP(recipient: Address, message: Message, listeningPort: Int, session: Session): Package {
        val serializedMessage = message.serialize()
        val metadata = PackageMetadata(listeningPort, session)
        val pkg = Package(serializedMessage, metadata)

        if (beforePackageSent != null)
            beforePackageSent!!(pkg)

        writePackage(pkg, recipient, maxPacketSizeBytes)

        return pkg
    }

    /**
     * Writes package to datagram packet
     */
    private fun writePackageUDP(pkg: Package, recipient: Address, maxPacketSizeBytes: Int) {
        val serializedPkg = Package.serialize(pkg)
                ?: throw IllegalArgumentException("Can not write empty package")

        val compressedAndSerializedPkg = CompressionUtils.compress(serializedPkg)

        if (maxPacketSizeBytes < compressedAndSerializedPkg.size)
            throw RuntimeException("Unable to send packages with size more than $maxPacketSizeBytes")

        val packet = DatagramPacket(compressedAndSerializedPkg, compressedAndSerializedPkg.size, recipient.toInetAddress(), recipient.port)
        logger.info("Sending: $pkg")

        clientSocketUDP.send(packet)
    }

    /**
     * Parses datagram packets and get packages
     */
    private fun readPackageUDP(datagramPacket: DatagramPacket): Package? {
        val serializedAndCompressedPkg = datagramPacket.data
        val serializedAndDecompressedPkg = CompressionUtils.decompress(serializedAndCompressedPkg)

        return Package.deserialize(serializedAndDecompressedPkg)
    }

    /**
     * Sets callback that executes just after package is received
     */
    fun setAfterPackageReceived(modifier: PackageModifier) {
        afterPackageReceived = modifier
    }

    /**
     * Sets callback that executes just before package is sent
     */
    fun setBeforePackageSent(modifier: PackageModifier) {
        beforePackageSent = modifier
    }
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

/**
 * Annotation that is used to mark methods of controller class that should handle requests
 *
 * @param type {String} when we receive message with this type, this method invocation is triggered
 */
@Target(AnnotationTarget.FUNCTION)
annotation class OnRequest(val type: String)

/**
 * Annotation that is used to mark methods of controller class that should pre-process responses
 *
 * @param type {String} when we receive message with this type, this method invocation is triggered
 */
@Target(AnnotationTarget.FUNCTION)
annotation class OnResponse(val type: String)