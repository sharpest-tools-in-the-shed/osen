package net.stits.osen.controller

import net.stits.osen.*
import org.springframework.stereotype.Service
import javax.annotation.PostConstruct


const val TOPIC_TEST = "TEST"
object TestMessageTypes {
    const val PING = "PING"
    const val PONG = "PONG"
}

data class PingPayload(val text: String = "test")

/**
 * Example controller
 *
 * You can autowire everything spring allows to autowire
 */
@P2PController(TOPIC_TEST)
class ExampleController(private val service: ExampleService) {

    @PostConstruct
    fun checkIfAutowireWorks() {
        service.doSomething()
    }

    /**
     * You can write handlers in any way you like:
     *      handlePing()
     *      handlePing(a: PingPayload)
     *      handlePing(b: Address)
     *      handlePing(x: PingPayload, y: Address)
     *      handlePing(sender: Address, payload: PingPayload)
     * just make sure you passing to it no more than 2 parameters, and sender is always Address (payload can be Any?)
     */
    @On(TestMessageTypes.PING)
    fun handlePing(payload: PingPayload, sender: Address) {
        println("RECEIVED PING REQUEST WITH PAYLOAD $payload, SENDING PONG BACK...")

        val message = Message(TOPIC_TEST, TestMessageTypes.PONG, "Pong string payload")
        P2P.send(sender, message, 1337)
    }

    @On(TestMessageTypes.PONG)
    fun handlePong(payload: String, sender: Address) {
        println("RECEIVED PONG REQUEST WITH PAYLOAD $payload - THAT HOST IS ALIVE")
    }
}

@Service
class ExampleService {
    fun doSomething() {
        println("I did something")
    }
}