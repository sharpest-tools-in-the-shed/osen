package net.stits.osen

import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner
import kotlinx.coroutines.experimental.launch
import java.lang.reflect.Method
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.nio.charset.StandardCharsets

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
 * Composite object that do all the stuff.
 * On construction it scans all packages (or package you pass to it) and finds classes annotated with @P2PController,
 * then it instantiates that controller and finds all methods of this class with @On annotation and saves this information.
 * Then it starts UDP-server (handle thread separation by yourself) on specified port (u can start many of it in test purposes)
 * and listens to packets.
 * Every packet is transformed (bytes -> json) into net.stits.osen.Package object that contains net.stits.osen.Message
 * object that contains Topic and Type which are used to determine what controller method should be invoked.
 *
 * @param listeningPort {Int} port to listen to UPD-packets
 * @param maxPacketSizeBytes {Int} maximum size of packet // ignored for now
 * @param packageToScan {String} if you have multiple networks with different controllers for each, place them in
 * different packages and specify package for each network
 */
class P2P(private val listeningPort: Int, private val maxPacketSizeBytes: Int = 1024, packageToScan: String? = null) {
    private val topicHandlers: TopicHandlers = hashMapOf()

    init {
        val scanner: FastClasspathScanner = if (packageToScan == null)
            FastClasspathScanner()
        else
            FastClasspathScanner(packageToScan)

        scanner.matchClassesWithAnnotation(P2PController::class.java) { controller ->
            val messageTopic = controller.getAnnotation(P2PController::class.java).topic
            println("Found P2P controller: ${controller.canonicalName} (topic: $messageTopic)")

            val onMethods = controller.methods.filter { method -> method.isAnnotationPresent(On::class.java) }
            val listeners = hashMapOf<String, Method>()

            onMethods.forEach { method ->
                val messageType = method.getAnnotation(On::class.java).type
                val methodArgs = method.parameters.map { "${it.name}:${it.type}" }

                println("\tFound @On annotated method: ${method.name} (type: $messageType)")
                if (methodArgs.isNotEmpty())
                    println("\t - Parameters: ${methodArgs.joinToString(", ")}")

                listeners[messageType] = method
            }

            val topicController = TopicController(controller.newInstance(), listeners)
            topicHandlers[messageTopic] = topicController
        }

        scanner.scan()

        println("Handlers parsed successfully, initializing network...")

        initNetwork()
    }

    private fun initNetwork() {
        val serverSocket = DatagramSocket(listeningPort)
        val packet = DatagramPacket(ByteArray(maxPacketSizeBytes), maxPacketSizeBytes)

        println("Listening on $listeningPort") // TODO: change to logger

        while (true) {
            serverSocket.receive(packet)

            val recipient = Address(packet.address.hostAddress, packet.port)
            println("Got connection from: $recipient") // TODO: change to logger

            val pkg = readPackage(packet)

            if (pkg == null) {
                println("Received an empty package")
                continue
            }

            println("Read $pkg from $recipient")

            val actualRecipient = Address(recipient.host, pkg.metadata.port)
            println("$recipient is actually $actualRecipient")

            val topic = pkg.message.topic
            val type = pkg.message.type

            val topicHandler = topicHandlers[topic]!! // TODO
            val messageHandler = topicHandler.listeners[type]!! // TODO

            // TODO: add parameters determinition
            // TODO: maybe make only Address determinible (so we can remove Payload and use any other class as payload)
            val payloadParameter = messageHandler.parameters
                    .find { parameter ->
                        Payload::class.java.isAssignableFrom(parameter.type)
                    }

            // TODO: add [] payload handling
            val message = pkg.message.deserialize(payloadParameter!!.type)

            launch {
                messageHandler.invoke(topicHandler.controller, message.payload, actualRecipient)
            }
        }
    }

    private fun readPackage(datagramPacket: DatagramPacket): Package? {
        return Package.deserialize(datagramPacket.data)
    }

    companion object {
        private val clientSocket = DatagramSocket()

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

            println("Sent $pkg to $recipient")
        }

        private fun writePackage(pkg: Package, recipient: Address, maxPacketSizeBytes: Int) {
            val serializedPkg = Package.serialize(pkg)
                    ?: throw IllegalArgumentException("Can not write empty package")

            if (maxPacketSizeBytes < serializedPkg.size)
                throw RuntimeException("Unable to send packages with size more than $maxPacketSizeBytes")

            val packet = DatagramPacket(serializedPkg, serializedPkg.size, recipient.getInetAddress(), recipient.port)
            println("Sending: ${packet.data.toString(StandardCharsets.UTF_8)}")
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
