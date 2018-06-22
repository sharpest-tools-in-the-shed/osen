package net.stits.osen.controller

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import net.stits.osen.*


const val TOPIC_TEST = "TEST"
object TestMessageTypes {
    const val PING = "PING"
    const val PONG = "PONG"
}

data class PongPayload(val text1: String = "test") : Payload
data class PingPayload(val text: String = "test") : Payload


@P2PController(TOPIC_TEST)
class ExampleController {
    val mapper = ObjectMapper().registerModule(KotlinModule())

    @On(TestMessageTypes.PING)
    fun handlePing(payload: PingPayload, sender: Address) {
        println("RECEIVED PING REQUEST WITH PAYLOAD $payload, SENDING PONG BACK...")

        val addr = Address("localhost", 1337)
        val message = Message(TOPIC_TEST, TestMessageTypes.PONG, PongPayload())
        P2P.send(addr, message, 1337)
    }

    @On(TestMessageTypes.PONG)
    fun handlePong(payload: PongPayload, sender: Address) {
        println("RECEIVED PONG REQUEST WITH PAYLOAD $payload - THAT HOST IS ALIVE")
    }
}
