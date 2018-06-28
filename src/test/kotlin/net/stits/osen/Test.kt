package net.stits.osen

import net.stits.osen.controller.TOPIC_TEST
import net.stits.osen.controller.TestMessageTypes
import net.stits.osen.controller.TestPayload
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner


@RunWith(SpringJUnit4ClassRunner::class)
@ContextConfiguration(classes = [SecondApplication::class])
class Test {

    @Test
    fun doTest() {
        val message = Message(TOPIC_TEST, TestMessageTypes.TEST, TestPayload())
        val response = P2P.sendRequest(Address("localhost", 1337), message, 1338, String::class.java)

        println("Got response: $response omg!")
    }
}