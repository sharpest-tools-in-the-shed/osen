package net.stits.osen.controller

import net.stits.osen.*


const val TOPIC_TEST = "TEST"
object TestMessageTypes {
    const val PING = "PING"
    const val PONG = "PONG"
}

data class PingPayload(val text: String = "test")

@P2PController(TOPIC_TEST)
class ExampleController {

    /**
     * You can write handlers in any way you like:
     *      handlePing()
     *      handlePing(a: PingPayload)
     *      handlePing(b: Address)
     *      handlePing(x: PingPayload, y: Address)
     *      handlePing(sender: Address, payload: PingPayload)
     * just make sure you passing to it no more than 2 parameters, and sender is always Address (payload can be Any)
     */
    @On(TestMessageTypes.PING)
    fun handlePing(payload: PingPayload, sender: Address) {
        println("RECEIVED PING REQUEST WITH PAYLOAD $payload, SENDING PONG BACK...")

        val addr = Address("localhost", 1337)
        val message = Message(TOPIC_TEST, TestMessageTypes.PONG, "Pong string payload")
        P2P.send(addr, message, 1337)
    }

    @On(TestMessageTypes.PONG)
    fun handlePong(payload: String, sender: Address) {
        println("RECEIVED PONG REQUEST WITH PAYLOAD $payload - THAT HOST IS ALIVE")
    }
}
