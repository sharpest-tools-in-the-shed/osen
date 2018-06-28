// TODO: update readme

## Spring P2P extension
This extension enables you to write applications which can exchange UDP messages in springmvc-way.
Zero-dependency (actually has only three: kotlin-coroutines, spring-context and jackson)

#### By example
Let's suppose you need to implement some very heavy protocol called Example Protocol.
This protocol has all of TWO types of messages: Ping and Pong. 
The flow is also very complicated and heavy: every peer that receives Ping should reply Pong back.

Okay. Here we go.
Define controller and handle message types.

```kotlin
@P2PController("EXAMPLE")
class ExampleController {
    @On("PING")
    fun handlePing(sender: Address) {
        println("RECEIVED PING OMG, SENDING PONG BACK...")

        val message = Message("EXAMPLE", "PONG")
        P2P.send(sender, message, SOME_PORT_WE_ARE_LISTENING_TO)
        println("SENT PONG IM SUCH A NICE GUY")
    }

    @On("PONG")
    fun handlePong(sender: Address) {
        println("RECEIVED PONG - THAT HOST IS ALIVE SO SWEET")
    }
}
```

Yes, just like in SpringMVC. Then somewhere in code, when you need to send a ping to someone, drop:
```kotlin
val receiver = Address(RECEIVER_HOST_ACCUIRED_SOMEWAY, PORT_RECEIVER_IS_LISTENING_TO)
val message = Message("EXAMPLE", "PING")

P2P.send(receiver, message, SOME_PORT_WE_ARE_LISTENING_TO) // static function
```

Then define spring bean and start spring app
```kotlin
    @SpringBootApplication
    open class Application {
        private val port = 1337
        private val packageToScan = "net.stits"
    
        @Bean
        open fun p2pInitializer(): P2P {
            return P2P(listeningPort = port, packageToScan = packageToScan)
        }
    }
    
    fun main(args: Array<String>) {
        SpringApplication.run(Application::class.java, *args)
    }
```

__OMG THAT IS SO NICE WOW__

Wanna send some payload? Okay.
```kotlin
@P2PController("EXAMPLE")
class ExampleController {
    @On("PING")
    fun handlePing(payload: PingPayload, sender: Address) {
        println("RECEIVED PING WITH PAYLOAD $payload, SENDING PONG BACK...")

        val message = Message("EXAMPLE", "PONG", PongPayload("Hey"))
        P2P.send(sender, message, SOME_PORT_WE_ARE_LISTENING_TO)
        println("SENT PONG IM SUCH A NICE GUY")
    }

    @On("PONG")
    fun handlePong(sender: Address, something: PongPayload) {
        println("RECEIVED PONG WITH PAYLOAD $something - THAT HOST IS ALIVE SO SWEET")
    }
}

data class PingPayload(val pingText: String = "test")
data class PongPayload(val pongText: String = "test")
```

Then somewhere:
```kotlin
val receiver = Address(RECEIVER_HOST_ACCUIRED_SOMEWAY, PORT_RECEIVER_IS_LISTENING_TO)
val message = Message("EXAMPLE", "PING", PingPayload("Hello")

P2P.send(receiver, message, SOME_PORT_WE_ARE_LISTENING_TO)
```

And ofc you can autowire everything in your p2p-controller.

Examples are in the test package. Technical details are in javadocs. Good luck.