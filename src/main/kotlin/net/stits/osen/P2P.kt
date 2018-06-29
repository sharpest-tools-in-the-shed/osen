package net.stits.osen

import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.runBlocking
import kotlinx.coroutines.experimental.withTimeoutOrNull
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider
import org.springframework.context.support.GenericApplicationContext
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

/**
 * Object containing controller and mapping [Message type -> Method that handles this message type]
 * Only one handler per unique together Topic and Type is possible right now.
 *
 * TODO: maybe switch to concurrent hash map
 * TODO: maybe add support for multiple handlers per topic-type
 *
 * @param controller {Any} @P2PController-annotated instance
 * @param listeners {String -> Method} mapping [MessageType -> @On method]
 */
data class TopicController(val controller: Any, val listeners: Map<String, Method>)

/**
 * On construction it scans all packages (or package you pass to it) and finds classes annotated with @P2PController,
 * adds them to Spring Context, then it finds all methods of this class with @On annotation and saves this information.
 * Then it starts UDP-server on specified port and listens for packets.
 * Every packet is transformed (bytes -> json) into net.stits.osen.Package object that contains net.stits.osen.Message
 * object that contains Topic and Type which are used to determine what controller method should be invoked.
 *
 * @param packageToScan {String} you should specify this in order to say Spring classpath scanner where do
 * your @P2PControllers placed // TODO: spring by itself scans packages without this somehow
 * @param listeningPort {Int} port to listen for UPD-packets
 * @param maxPacketSizeBytes {Int} maximum size of packet // TODO: make this work or remove
 */
class P2P(private val listeningPort: Int, private val packageToScan: String, private val maxPacketSizeBytes: Int = 1024) {
    private val topicHandlers: TopicHandlers = hashMapOf()
    private val clientSocket = DatagramSocket()

    @Autowired
    lateinit var context: GenericApplicationContext

    /**
     * This map is used to store responses. After "send" with specified _class invoked it is watching this map for a
     * response (session id is used as key) and returns it when response arrives.
     *
     * TODO: maybe make concurrent
     */
    val responses = hashMapOf<Int, Any?>()

    /**
     * This method is invoked by Spring itself. It scans through specified package, finds all needed classes and methods
     * and then initializes network
     */
    @PostConstruct
    private fun initBySpring() {
        val provider = ClassPathScanningCandidateComponentProvider(false)
        provider.addIncludeFilter(AnnotationTypeFilter(P2PController::class.java))

        val beanDefinitions = provider.findCandidateComponents(packageToScan)
        beanDefinitions.forEach { beanDefinition ->

            // adding found @P2PControllers to spring context
            val beanClass = Class.forName(beanDefinition.beanClassName)
            context.registerBean(beanClass)

            // getting topic name
            val messageTopic = beanClass.getAnnotation(P2PController::class.java).topic
            logger.info("Found P2P controller: ${beanClass.canonicalName} (topic: $messageTopic)")

            // finding @On methods
            val listeners = getAllAnnotatedMethodsOfBean(beanClass)

            // adding instances of @P2PControllers to list of topic handlers
            val beanInstance = context.getBean(beanClass)
            val topicController = TopicController(beanInstance, listeners)
            topicHandlers[messageTopic] = topicController
        }

        logger.info("Spring P2P extension successfully initialized, starting network up...")

        thread {
            initNetwork()
        }
    }

    private fun getAllAnnotatedMethodsOfBean(beanClass: Class<*>): Map<String, Method> {
        val onMethods = beanClass.methods.filter { method -> method.isAnnotationPresent(On::class.java) }
        val listeners = hashMapOf<String, Method>()

        onMethods.forEach { method ->
            val messageType = method.getAnnotation(On::class.java).type
            val methodArgs = method.parameters.map { "${it.name}:${it.type}" }

            logger.info("\tFound @OnRequest annotated method: ${method.name} (type: $messageType)")
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

            handleOnInvocation(topicHandler, topic, type, pkg.message, actualRecipient, session)
        }
    }

    private fun handleOnInvocation(topicHandler: TopicController, topic: String, type: String, message: SerializedMessage, recipient: Address, session: Session) {
        val messageHandler = topicHandler.listeners[type]
        if (messageHandler == null) {
            logger.warning("No method to handle message of type: $type of topic: $topic, skipping...")
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

        logger.info("Trying to invoke method: ${messageHandler.name} with arguments: ${arguments.toList()}")

        launch {
            when (session.getStage()) {
                SessionStage.REQUEST -> messageHandler.invoke(topicHandler.controller, *arguments)
                SessionStage.RESPONSE -> responses[session.id] = messageHandler.invoke(topicHandler.controller, *arguments)
                else -> throw RuntimeException("Session ${session.id} is in invalid stage: ${session.getStage()}. Unable to invoke ${messageHandler.name}.")
            }
        }
    }

    private fun readPackage(datagramPacket: DatagramPacket): Package? {
        return Package.deserialize(datagramPacket.data)
    }

    companion object {
        val logger = loggerFor(Message::class.java)
    }

    /**
     * Static function that is used to send messages to other peers
     * Parameters _class and _session are used to determine whether you requesting or not information from peer
     * If _class specified, new session will be generated and remote peer can receive it with controller method parameters
     * then they can send it back along with a message. When we receive our session back, we execute @On method, get
     * it's return value and return it from here. Little bit complicated (idk how to explain it even to myself, so forgive me).
     * It helps to keep request processing logic in controllers and use return value everywhere we need.
     *
     *
     * Some scheme, maybe it will help:
     *
     * 1. [local machine] send(address, message, port, _class = String::class.java) //blocks after this ->
     * 2. [remote machine] @On(sender, payload, session) // it's like request handler now ->
     * 3. [remote machine] send(address, message, port, _session = session) ->
     * 4. [local machine] @On(sender, payload): String // it's like response handler now ->
     * 5. [local machine] // send from step 1 unblocks and returns String
     *
     *
     * @param recipient {net.stits.osen.Address} message recipient
     * @param message {net.stits.osen.Message} message itself
     * @param listeningPort {Int} port which you listen to so remote peer can send us messages too
     * @param _class {Class<T>} - specify this if you expect to receive some payload of type T back
     * @param _session {Session} - specify this if you sending a response (you should have it from @On method parameter)
     *
     * @return {Any?} some payload you return from @On method
     */
    fun send(recipient: Address, message: Message, listeningPort: Int, _class: Class<out Any?>? = null, _session: Session? = null, timeout: Long = 30000): Any? {
        if (_class == null) {
            if (_session != null) {
                if (_session.getStage() == SessionStage.CONSUMED) {
                    throw IllegalArgumentException("Session ${_session.id} is expired.")
                }

                if (_session.getStage() == SessionStage.REQUEST) {
                    _session.processLifecycle()
                }

                val pkg = sendMessage(recipient, message, listeningPort, _session)
                logger.info("Sent $pkg to $recipient with session: $_session")

                return null
            } else {
                val session = Session.createInactiveSession()
                val pkg = sendMessage(recipient, message, listeningPort, session)
                logger.info("Sent $pkg to $recipient with inactive session")

                return null
            }
        }

        if (_session != null) throw IllegalArgumentException(
                "Specifying _session means that you want to send something in response. " +
                        "Specifying _class means that you want to request for something. " +
                        "Choose one, please."
        )

        return runBlocking {
            val session = Session.createSession()

            val pkg = sendMessage(recipient, message, listeningPort, session)
            logger.info("Sent $pkg to $recipient, waiting for response...")

            val delay = 5
            val response = withTimeoutOrNull(timeout) {
                repeat(timeout.div(delay).toInt()) {
                    if (responses.containsKey(session.id)) {
                        val response = responses[session.id]

                        logger.info("Received response: $response")
                        return@withTimeoutOrNull response
                    } else {
                        delay(delay)
                    }
                }
            }

            responses.remove(session.id)

            _class.cast(response)
        }
    }

    private fun sendMessage(recipient: Address, message: Message, listeningPort: Int, session: Session): Package {
        val metadata = PackageMetadata(listeningPort, session)
        val pkg = Package(message.serialize(), metadata)
        writePackage(pkg, recipient, maxPacketSizeBytes)

        return pkg
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
