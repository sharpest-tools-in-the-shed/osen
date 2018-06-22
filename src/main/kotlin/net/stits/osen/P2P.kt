package net.stits.osen

import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner
import kotlinx.coroutines.experimental.launch
import java.lang.reflect.Method
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.nio.charset.StandardCharsets


typealias TopicHandlers = HashMap<String, TopicController>

data class TopicController(val controller: Any, val listeners: HashMap<String, Method>)

class P2P(private val listeningPort: Int, private val maxPacketSizeBytes: Int = 1024, packageToScan: String? = null) {
    private val topicHandlers: TopicHandlers = hashMapOf()

    init {
        val scanner: FastClasspathScanner = if (packageToScan == null)
            FastClasspathScanner()
        else
            FastClasspathScanner(packageToScan)

        scanner.matchClassesWithAnnotation(P2PController::class.java) { controller ->
            val messageTopic = controller.getAnnotation(P2PController::class.java).topic
            println("Found net.stits.osen.P2P controller: ${controller.canonicalName} (topic: $messageTopic)")

            val onMethods = controller.methods.filter { method -> method.isAnnotationPresent(On::class.java) }
            val listeners = hashMapOf<String, Method>()

            onMethods.forEach { method ->
                val messageType = method.getAnnotation(On::class.java).type
                val methodArgs = method.parameters.map { "${it.name}:${it.type}" }

                println("\tFound @net.stits.osen.On annotated method: ${method.name} (type: $messageType)")
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


            val payloadParameter = messageHandler.parameters
                    .find { parameter ->
                        Payload::class.java.isAssignableFrom(parameter.type)
                    }

            // TODO: add [] payload handling
            val message = pkg.message.deserialize(payloadParameter!!.type)

            messageHandler.invoke(topicHandler.controller, message.payload, actualRecipient) // TODO: serialization =CCC
        }

        serverSocket.close()
    }

    private fun readPackage(datagramPacket: DatagramPacket): Package? {
        return Package.deserialize(datagramPacket.data)
    }

    companion object {
        private val clientSocket = DatagramSocket()

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

@Target(AnnotationTarget.CLASS)
annotation class P2PController(val topic: String)

@Target(AnnotationTarget.FUNCTION)
annotation class On(val type: String)
