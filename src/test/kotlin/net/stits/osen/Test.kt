package net.stits.osen

import net.stits.osen.controller.TOPIC_TEST
import net.stits.osen.controller.TestMessageTypes
import net.stits.osen.controller.TestPayload
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner


@RunWith(SpringJUnit4ClassRunner::class)
//@ContextConfiguration(classes = [SecondApplication::class])
@SpringBootTest
class Test {

    @Test
    fun doTest() {
        val message = Message(TOPIC_TEST, TestMessageTypes.TEST_REQ, TestPayload())
        val receiver = Address("localhost", 1337)
        val response = P2P.send(receiver, message, 1337, _class = String::class.java)

        println("Got response: $response omg!")
    }
}