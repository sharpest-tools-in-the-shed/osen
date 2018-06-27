package net.stits.osen

import kotlinx.coroutines.experimental.launch
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
 * @param listeners {String -> Method} mapping [MessageType -> @Any-annotated method]
 */
data class TopicController(val controller: Any, val listeners: HashMap<String, Method>)

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
class P2P(private val packageToScan: String, private val listeningPort: Int = 1337, private val maxPacketSizeBytes: Int = 1024): ApplicationContextAware {
    private val topicHandlers: TopicHandlers = hashMapOf()
    private var applicationContext: ApplicationContext? = null

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
            val onMethods = beanClass.methods.filter { method -> method.isAnnotationPresent(On::class.java) }
            val listeners = hashMapOf<String, Method>()

            onMethods.forEach { method ->
                val messageType = method.getAnnotation(On::class.java).type
                val methodArgs = method.parameters.map { "${it.name}:${it.type}" }

                logger.info("\tFound @On annotated method: ${method.name} (type: $messageType)")
                if (methodArgs.isNotEmpty())
                    logger.info("\t - Parameters: ${methodArgs.joinToString(", ")}")

                listeners[messageType] = method
            }

            // adding instances of @P2PControllers to list of topic handlers
            val beanInstance = applicationContext!!.getBean(beanClass)
            val topicController = TopicController(beanInstance, listeners)
            topicHandlers[messageTopic] = topicController
        }

        logger.info("Spring P2P extension successfully initialized, starting network up...")

        thread {
            initNetwork()
        }
    }

    override fun setApplicationContext(applicationContext: ApplicationContext?) {
        if (applicationContext == null)
            throw IllegalArgumentException("Application context can't be null")

        this.applicationContext = applicationContext
    }

    private fun initNetwork() {
        val serverSocket = DatagramSocket(listeningPort)
        val packet = DatagramPacket(ByteArray(maxPacketSizeBytes), maxPacketSizeBytes)

        logger.info("Listening to UDP packets on port: $listeningPort")

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

            val topicHandler = topicHandlers[topic]
            if (topicHandler == null) {
                logger.warning("No controller for topic $topic, skipping...")
                continue
            }

            val messageHandler = topicHandler.listeners[type]
            if (messageHandler == null) {
                logger.warning("No method to handle message type $type of topic $topic, skipping...")
                continue
            }

            if (messageHandler.parameters.size > 2) {
                logger.warning("Method ${messageHandler.name} of class ${topicHandler.controller} has more then 2 arguments, skipping...")
                continue
            }

            val arguments = Array(messageHandler.parameters.size) { index ->
                val parameter = messageHandler.parameters[index]

                if (Address::class.java.isAssignableFrom(parameter.type))
                    actualRecipient
                else {
                    if (pkg.message.payload.isEmpty())
                        null
                    else
                        pkg.message.deserialize(parameter.type).payload
                }
            }

            launch { messageHandler.invoke(topicHandler.controller, *arguments) }
        }
    }

    private fun readPackage(datagramPacket: DatagramPacket): Package? {
        return Package.deserialize(datagramPacket.data)
    }

    companion object {
        private val clientSocket = DatagramSocket()
        val logger = loggerFor(Message::class.java)

        /**
         * Static function that is used to send messages to other peers
         *
         * @param recipient {net.stits.osen.Address} message recipient
         * @param message {net.stits.osen.Message} message itself
         * @param listeningPort {Int} port which you listen to so remote peer can send us messages too
         * @param maxPacketSizeBytes {Int} maximum package size in bytes (throws exception if result package size > this value)
         */
        fun send(recipient: Address, message: Message, listeningPort: Int, maxPacketSizeBytes: Int = 1024) = launch {
            val metadata = PackageMetadata(listeningPort)
            val pkg = Package(message.serialize(), metadata)
            writePackage(pkg, recipient, maxPacketSizeBytes)

            logger.info("Sent $pkg to $recipient")
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
 * Annotation that is used to mark methods of controller classes that should handle some type of messages
 *
 * @param type {String} when we receive message with this type, this method invocation is triggered
 */
@Target(AnnotationTarget.FUNCTION)
annotation class On(val type: String)
