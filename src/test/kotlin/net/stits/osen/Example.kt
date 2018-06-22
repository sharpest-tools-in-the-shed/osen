package net.stits.osen

import net.stits.osen.controller.PingPayload
import net.stits.osen.controller.TOPIC_TEST
import net.stits.osen.controller.TestMessageTypes
import kotlin.concurrent.thread

fun main(args: Array<String>) {
    drawBanner()

    thread {
        P2P(listeningPort = 1337, packageToScan = "net.stits.osen.controller")
    }

    val addr = Address("localhost", 1337)
    val message = Message(TOPIC_TEST, TestMessageTypes.PING, PingPayload())
    P2P.send(addr, message, 1337)
}