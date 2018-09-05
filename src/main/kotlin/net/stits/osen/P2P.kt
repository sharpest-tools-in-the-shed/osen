package net.stits.osen

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.config.BeanDefinition
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider
import org.springframework.context.support.GenericApplicationContext
import org.springframework.core.type.filter.AnnotationTypeFilter
import java.lang.reflect.Method
import javax.annotation.PostConstruct
import kotlin.concurrent.thread

/**
 * TODO: add annotation @SpringP2PApplication(port) that starts network when annotating some class - this is not possible (or you need to change spring initialization procedure)
 *
 * On construction it scans all packages (or package you pass to it) and finds classes annotated with @P2PController,
 * adds them to Spring Context, then it finds all methods of this class with annotations and saves this information.
 * Then it starts UDP-server on specified port and listens for packets.
 * Every packet is transformed (bytes -> json) into net.stits.osen.Package object that contains net.stits.osen.Message
 * object that contains Topic, Type and Session which are used to determine what controller method should be invoked.
 *
 * @param basePackages {Array<String>} - you should specify this in order to say Spring classpath scanner where do
 * your @P2PControllers placed // TODO: somehow get this values from spring
 */
class P2P(private val basePackages: Array<String>) {
    @Autowired
    lateinit var context: GenericApplicationContext

    var listeningPort: Int = 1337

    private val topicHandlers: TopicHandlers = hashMapOf()
    lateinit var tcp: TCP

    companion object {
        val logger = loggerFor<P2P>()
    }

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

        tcp = TCP(listeningPort)
        initTCP()
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

            // adding instances of @P2PControllers to list of topic handlers
            val beanInstance = context.getBean(beanClass)
            val topicController = TopicController(beanInstance, onListeners)
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

            val signatureError = "@On annotated method signature is (a: <Any payload type>, b: Address) -> Any? | Unit"
            assertArgumentsPresentAt(beanClass, method) { signatureError }

            listeners[messageType] = method
        }

        return listeners
    }

    /**
     * Initializes network (listens for packets and executes controller methods).
     */
    private fun initTCP() {
        thread {
            tcp.listen { pkg, peer ->
                val topic = pkg.message.topic
                val type = pkg.message.type
                val topicHandler = topicHandlers[topic]

                if (topicHandler == null) {
                    logger.warning("No controller or flow for topic $topic, skipping...")
                    return@listen null
                }

                when (pkg.metadata.flag) {
                    Flags.MESSAGE -> {
                        handleOnInvocation(topicHandler, topic, type, pkg.message, peer) {
                            if (checkHasReturnType(it)) {
                                logger.warning("Method: $it has return type, but shouldn't. Skipping...")
                                return@handleOnInvocation false
                            }
                            return@handleOnInvocation true
                        }
                        return@listen null
                    }
                    Flags.REQUEST -> {
                        return@listen handleOnInvocation(topicHandler, topic, type, pkg.message, peer) {
                            if (!checkHasReturnType(it)) {
                                logger.warning("Method: $it doesn't have return type, but should. Skipping...")
                                return@handleOnInvocation false
                            }
                            return@handleOnInvocation true
                        }
                    }
                    Flags.RESPONSE -> {
                        tcp.addResponse(pkg.metadata.requestId!!, pkg.message.payload)
                        return@listen null
                    }
                    else -> {
                        logger.warning("Invalid flag passed!")
                        return@listen null
                    }
                }
            }
        }
    }

    /**
     * Finds needed @On annotated method and executes it with correct arguments
     */
    private fun handleOnInvocation(
            topicHandler: TopicController,
            topic: String,
            type: String,
            message: SerializedMessage,
            recipient: Address,
            assertion: ((method: Method) -> Boolean)? = null
    ): Any? {

        val messageHandler = topicHandler.onListeners[type]

        if (messageHandler == null) {
            logger.warning("No @On annotated method to handle message of type: $type of topic: $topic, skipping...")
            return null
        }

        if (assertion?.invoke(messageHandler) == false) return null

        val arguments = parseMethodArguments(messageHandler, recipient, message)

        logger.info("Trying to invoke method: ${messageHandler.name} with arguments: ${arguments.toList()}")
        return messageHandler.invoke(topicHandler.controller, *arguments)
    }

    private fun checkHasReturnType(method: Method) = method.returnType != Void.TYPE

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
                            ?: error("Payload (${message.payload}) deserialization failure, skipping...")
                }
            }
        }
    }

    /**
     * This function is used for a stateless message exchange. It triggers @On annotated method of controller.
     */
    suspend fun send(recipient: Address, message: Message) {
        tcp.sendTo(recipient, message)
    }

    /**
     * This functions are just shorthands for sendAndReceive()
     */
    suspend inline fun <reified T : Any> sendAndReceive(recipient: Address, message: Message): T {
        return tcp.sendAndReceive(recipient, message, T::class.java)
    }

    fun addBeforeMessageSent(topic: MessageTopic, modifier: PackageModifier) = tcp.putBeforeMessageSent(topic, modifier)
    fun addAfterMessageReceived(topic: MessageTopic, modifier: PackageModifier) = tcp.putAfterMessageReceived(topic, modifier)
}
