
_It's very like Spring but Osen._

## OSEN - Kotlin P2P messaging made easy
Library to send some UDP-messages and easily process ingoing ones. Implement multiple network protocols in one application
in rapid way.

#### By example
Let's suppose you need to implement some very heavy protocol called Example Protocol.
This protocol has all of TWO types of messages: Ping and Pong. 
The flow is also very complicated and heavy: every peer that receives Ping should reply Pong back.

Okay. Here we go.
Define controller and handle message types.

```
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

Yes, just like in Spring. Then somewhere in code, when you need to send a ping to someone, drop:
```
val receiver = Address(RECEIVER_HOST_ACCUIRED_SOMEWAY, PORT_RECEIVER_IS_LISTENING_TO)
val message = Message("EXAMPLE", "PING")

P2P.send(receiver, message, SOME_PORT_WE_ARE_LISTENING_TO) // static function
```

Then in program entrypoint (or in some spring component with @PostConstruct, or somewhere where code is actually executes):
```
    // thread { P2P(listeningPort = SOME_PORT_WE_ARE_LISTENING_TO) }
    P2P(listeningPort = SOME_PORT_WE_ARE_LISTENING_TO)
```

__OMG THAT IS SO NICE WOW__

Wanna send some payload? Okay.
```
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
```
val receiver = Address(RECEIVER_HOST_ACCUIRED_SOMEWAY, PORT_RECEIVER_IS_LISTENING_TO)
val message = Message("EXAMPLE", "PING", PingPayload("Hello")

P2P.send(receiver, message, SOME_PORT_WE_ARE_LISTENING_TO)
```

Example is in test package. Technical details are in javadocs. Good luck.