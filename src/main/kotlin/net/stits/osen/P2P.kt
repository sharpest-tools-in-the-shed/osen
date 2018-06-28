package net.stits.osen

import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.runBlocking
import kotlinx.coroutines.experimental.withTimeoutOrNull
import org.springframework.beans.factory.config.BeanDefinition.SCOPE_SINGLETON
import org.springframework.beans.factory.support.BeanDefinitionRegistry
import org.springframework.beans.factory.support.GenericBeanDefinition
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationContextAware
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider
import org.springframework.core.type.filter.AnnotationTypeFilter
import java.lang.reflect.Method
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.nio.charset.StandardCharsets
import java.util.*
import javax.annotation.PostConstruct
import kotlin.concurrent.thread


/**
 * Mapping [Message topic -> Controller that handles this topic]
 */
typealias TopicHandlers = HashMap<String, TopicController>

typealias AnnotationPropertyExtractor = (method: Method, annotation: Class<out Annotation>) -> String

/**
 * Object containing controller and mapping [Message type -> Method that handles this message type]
 * Only one handler per unique together Topic and Type is possible right now.
 *
 * TODO: maybe switch to concurrent hash map
 * TODO: maybe add support for multiple handlers per topic-type
 *
 * @param controller {Any} @P2PController-annotated instance
 * @param requestListeners {String -> Method} mapping [MessageType -> @OnRequest method]
 * @param responseListeners {String -> Method} mapping [MessageType -> @OnResponse method]
 */
data class TopicController(val controller: Any, val requestListeners: Map<String, Method>, val responseListeners: Map<String, Method>)

/**
 * On construction it scans all packages (or package you pass to it) and finds classes annotated with @P2PController,
 * adds them to Spring Context, then it finds all methods of this class with @On annotation and saves this information.
 * Then it starts UDP-server on specified port (u can start many of it in test purposes) and listens for packets.
 * Every packet is transformed (bytes -> json) into net.stits.osen.Package object that contains net.stits.osen.Message
 * object that contains Topic and Type which are used to determine what controller method should be invoked.
 *
 * @param packageToScan {String} you should specify this in order to say Spring classpath scanner where do
 * your @P2PControllers placed // TODO: spring by itself scans packages without this somehow
 * @param listeningPort {Int} port to listen for UPD-packets
 * @param maxPacketSizeBytes {Int} maximum size of packet // TODO: make this work or remove
 */
class P2P(private val packageToScan: String, private val listeningPort: Int, private val maxPacketSizeBytes: Int = 1024) : ApplicationContextAware {
    private val topicHandlers: TopicHandlers = hashMapOf()

    val responses = hashMapOf<Int, Any?>()

    @PostConstruct
    fun initBySpring() {
        val provider = ClassPathScanningCandidateComponentProvider(false)
        provider.addIncludeFilter(AnnotationTypeFilter(P2PController::class.java))

        val beanDefinitions = provider.findCandidateComponents("net.stits")
        beanDefinitions.forEach { beanDefinition ->

            // adding found @P2PControllers to spring context
            val beanClass = Class.forName(beanDefinition.beanClassName)
            val registry = applicationContext!!.autowireCapableBeanFactory as BeanDefinitionRegistry

            val newBeanDefinition = GenericBeanDefinition()
            newBeanDefinition.beanClass = beanClass
            newBeanDefinition.scope = SCOPE_SINGLETON

            registry.registerBeanDefinition(beanClass.canonicalName, newBeanDefinition)

            // getting topic name
            val messageTopic = beanClass.getAnnotation(P2PController::class.java).topic
            logger.info("Found P2P controller: ${beanClass.canonicalName} (topic: $messageTopic)")

            // finding @On methods
            val requestListeners = getAllOnRequestAnnotatedMethodsOfBean(beanClass)
            val responseListeners = getAllOnResponseAnnotatedMethodsOfBean(beanClass)

            // adding instances of @P2PControllers to list of topic handlers
            val beanInstance = applicationContext!!.getBean(beanClass)
            val topicController = TopicController(beanInstance, requestListeners, responseListeners)
            topicHandlers[messageTopic] = topicController
        }

        logger.info("Spring P2P extension successfully initialized, starting network up...")

        thread {
            initNetwork()
        }
    }

    override fun setApplicationContext(appContext: ApplicationContext?) {
        if (appContext == null)
            throw IllegalArgumentException("Application context can't be null")

        applicationContext = appContext
    }

    private fun getAllOnRequestAnnotatedMethodsOfBean(beanClass: Class<*>): Map<String, Method> {
        val onMethods = beanClass.methods.filter { method -> method.isAnnotationPresent(OnRequest::class.java) }
        val listeners = hashMapOf<String, Method>()

        onMethods.forEach { method ->
            val messageType = method.getAnnotation(OnRequest::class.java).type
            val methodArgs = method.parameters.map { "${it.name}:${it.type}" }

            logger.info("\tFound @OnRequest annotated method: ${method.name} (type: $messageType)")
            if (methodArgs.isNotEmpty())
                logger.info("\t - Parameters: ${methodArgs.joinToString(", ")}")

            listeners[messageType] = method
        }

        return listeners
    }

    private fun getAllOnResponseAnnotatedMethodsOfBean(beanClass: Class<*>): Map<String, Method> {
        val onMethods = beanClass.methods.filter { method -> method.isAnnotationPresent(OnResponse::class.java) }
        val listeners = hashMapOf<String, Method>()

        onMethods.forEach { method ->
            val messageType = method.getAnnotation(OnResponse::class.java).type
            val methodArgs = method.parameters.map { "${it.name}:${it.type}" }

            logger.info("\tFound @OnResponse annotated method: ${method.name} (type: $messageType)")
            if (methodArgs.isNotEmpty())
                logger.info("\t - Parameters: ${methodArgs.joinToString(", ")}")

            listeners[messageType] = method
        }

        return listeners
    }

    private fun initNetwork() {
        val serverSocket = DatagramSocket(listeningPort)
        val packet = DatagramPacket(ByteArray(maxPacketSizeBytes), maxPacketSizeBytes)

        logger.info("Listening for UDP packets on port: $listeningPort")

        while (true) {
            serverSocket.receive(packet)

            val recipient = Address(packet.address.hostAddress, packet.port)
            logger.info("Got connection from: $recipient")

            val pkg = readPackage(packet)

            if (pkg == null) {
                logger.warning("Received an empty package")
                continue
            }

            logger.info("Read $pkg from $recipient")

            val actualRecipient = Address(recipient.host, pkg.metadata.port)
            logger.info("$recipient is actually $actualRecipient")

            val topic = pkg.message.topic
            val type = pkg.message.type
            val session = pkg.metadata.session

            val topicHandler = topicHandlers[topic]
            if (topicHandler == null) {
                logger.warning("No controller for topic $topic, skipping...")
                continue
            }

            if (session != null) {
                if (responses.containsKey(session.id) && responses[session.id] == null)
                    handleOnResponseInvocation(topicHandler, topic, type, pkg.message, actualRecipient, session)
                else if (!responses.containsKey(session.id))
                    handleOnRequestInvocation(topicHandler, topic, type, pkg.message, actualRecipient, session)
                else
                    logger.warning("Inconsistent session state!")
            } else {
                handleOnRequestInvocation(topicHandler, topic, type, pkg.message, actualRecipient, null)
            }
        }
    }

    private fun handleOnRequestInvocation(topicHandler: TopicController, topic: String, type: String, message: SerializedMessage, recipient: Address, session: Session?) {
        val messageHandler = topicHandler.requestListeners[type]
        if (messageHandler == null) {
            logger.warning("No method to handle request of type $type of topic $topic, skipping...")
            return
        }

        if (messageHandler.parameters.size > 3) {
            logger.warning("Method ${messageHandler.name} of class ${topicHandler.controller} has more then 3 arguments, skipping...")
            return
        }

        val arguments = Array(messageHandler.parameters.size) { index ->
            val parameter = messageHandler.parameters[index]

            if (Address::class.java.isAssignableFrom(parameter.type))
                recipient
            else if (Session::class.java.isAssignableFrom(parameter.type)) {
                session
            } else {
                if (message.payload.isEmpty())
                    null
                else {
                    val deserializedMessage = message.deserialize(parameter.type)
                            ?: throw RuntimeException("Message deserialization failure, skipping...")
                    deserializedMessage.payload
                }
            }
        }

        launch { messageHandler.invoke(topicHandler.controller, *arguments) }
    }

    private fun handleOnResponseInvocation(topicHandler: TopicController, topic: String, type: String, message: SerializedMessage, recipient: Address, session: Session) {
        val messageHandler = topicHandler.responseListeners[type]

        if (messageHandler == null) {
            logger.warning("No method to handle response of type $type of topic $topic, skipping...")
            return
        }

        if (messageHandler.parameters.size > 2) {
            logger.warning("Method ${messageHandler.name} of class ${topicHandler.controller} has more then 3 arguments, skipping...")
            return
        }

        val arguments = Array(messageHandler.parameters.size) { index ->
            val parameter = messageHandler.parameters[index]

            if (Address::class.java.isAssignableFrom(parameter.type))
                recipient
            else {
                if (message.payload.isEmpty())
                    null
                else {
                    val deserializedMessage = message.deserialize(parameter.type)
                            ?: throw RuntimeException("Message deserialization failure, skipping...")
                    deserializedMessage.payload
                }
            }
        }

        logger.info("Trying to invoke method: ${messageHandler.name} with arguments: ${arguments.toList()}")

        launch {
            responses[session.id] = messageHandler.invoke(topicHandler.controller, *arguments)
        }
    }

    private fun readPackage(datagramPacket: DatagramPacket): Package? {
        return Package.deserialize(datagramPacket.data)
    }

    companion object {
        private var applicationContext: ApplicationContext? = null
        private val clientSocket = DatagramSocket()
        val logger = loggerFor(Message::class.java)

        private const val maxPacketSizeBytes: Int = 1024

        /**
         * Static function that is used to send messages to other peers
         *
         * @param recipient {net.stits.osen.Address} message recipient
         * @param message {net.stits.osen.Message} message itself
         * @param listeningPort {Int} port which you listen to so remote peer can send us messages too
         * @param clazz {Class<T>} - specify this if you expect to receive some payload of type T back
         */
        fun <T> sendRequest(recipient: Address, message: Message, listeningPort: Int, clazz: Class<T>? = null, timeout: Long = 30000): T? {
            if (clazz == null) {
                val metadata = PackageMetadata(listeningPort)
                val pkg = Package(message.serialize(), metadata)
                writePackage(pkg, recipient, maxPacketSizeBytes)

                logger.info("Sent $pkg to $recipient")
                return null
            }

            return runBlocking {
                val p2p = getP2PBeanFromContext()
                val sessionId = Random().nextInt(Int.MAX_VALUE)

                p2p.responses[sessionId] = null

                val metadata = PackageMetadata(listeningPort, Session(sessionId))
                val pkg = Package(message.serialize(), metadata)
                writePackage(pkg, recipient, maxPacketSizeBytes)

                logger.info("Sent $pkg to $recipient, waiting for response...")

                val response = withTimeoutOrNull(timeout) {
                    repeat(1000) {
                        val response = p2p.responses[sessionId]

                        if (response != null) {
                            logger.info("Received response: $response")
                            return@withTimeoutOrNull response
                        } else {
                            delay(30)
                        }
                    }
                }

                p2p.responses.remove(sessionId)

                clazz.cast(response)
            }
        }

        /**
         * Same as sendRequest, but used to answer some requests via known session
         */
        fun sendResponse(recipient: Address, message: Message, listeningPort: Int, session: Session) {
            val metadata = PackageMetadata(listeningPort, session)
            val pkg = Package(message.serialize(), metadata)
            writePackage(pkg, recipient, maxPacketSizeBytes)

            logger.info("Responded with $pkg to $recipient")
        }

        private fun getP2PBeanFromContext(): P2P {
            return applicationContext!!.getBean(P2P::class.java)
        }

        private fun writePackage(pkg: Package, recipient: Address, maxPacketSizeBytes: Int) {
            val serializedPkg = Package.serialize(pkg)
                    ?: throw IllegalArgumentException("Can not write empty package")

            if (maxPacketSizeBytes < serializedPkg.size)
                throw RuntimeException("Unable to send packages with size more than $maxPacketSizeBytes")

            val packet = DatagramPacket(serializedPkg, serializedPkg.size, recipient.getInetAddress(), recipient.port)
            logger.info("Sending: ${packet.data.toString(StandardCharsets.UTF_8)}")

            clientSocket.send(packet)
        }
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
annotation class OnRequest(val type: String)

/**
 * Annotation that is used to mark methods of controller class that should handle some type of responses
 *
 * @param type {String} when we receive response with this type, this method invocation is triggered
 */
@Target(AnnotationTarget.FUNCTION)
annotation class OnResponse(val type: String)