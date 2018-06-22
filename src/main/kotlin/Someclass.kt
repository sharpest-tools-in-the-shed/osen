import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule


@P2PController("_KAD")
class KademliaController {
    val mapper = ObjectMapper().registerModule(KotlinModule())

    @On("PING")
    fun handlePing(payload: PingPayload, sender: Address) {
        println("RECEIVED PING REQUEST WITH PAYLOAD $payload, SENDING PONG BACK...")

        val addr = Address("localhost", 1337)
        val message = Message("_KAD", "PONG", PongPayload())
        P2P.send(addr, message, 1337)
    }

    @On("PONG")
    fun handlePong(payload: PongPayload, sender: Address) {
        println("RECEIVED PONG REQUEST WITH PAYLOAD $payload - THAT HOST IS ALIVE")
    }
}

fun main(args: Array<String>) {
    drawBanner()

    val net = P2P(1337)

    val addr = Address("localhost", 1337)
    val message = Message("_KAD", "PING", PingPayload())
    P2P.send(addr, message, 1337)
}

fun drawBanner() {
    println(
                    "#######  #####  ####### #     # \n" +
                    "#     # #     # #       ##    # \n" +
                    "#     # #       #       # #   # \n" +
                    "#     #  #####  #####   #  #  # \n" +
                    "#     #       # #       #   # # \n" +
                    "#     # #     # #       #    ## \n" +
                    "#######  #####  ####### #     # \n" +
                    "--- P2P messaging framework --- \n"
    )
}

data class PongPayload(val text1: String = "text") : Payload
data class PingPayload(val text: String = "test") : Payload
