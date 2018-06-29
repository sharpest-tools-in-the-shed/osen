package net.stits.osen

import net.stits.osen.controller.TOPIC_TEST
import net.stits.osen.controller.TestMessageTypes
import net.stits.osen.controller.TestPayload
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner


@RunWith(SpringJUnit4ClassRunner::class)
@SpringBootTest
class Test {
    @Autowired
    lateinit var p2p: P2P
    private val receiver = Address("localhost", 1337)

    @Test
    fun `test single request and response`() {
        val message = Message(TOPIC_TEST, TestMessageTypes.TEST_REQUEST, TestPayload())
        val response = p2p.send(receiver, message, 1337, _class = String::class.java)

        assert(response == "Test string payload") { "send() returned invalid response" }
    }

    @Test
    fun `test null payload`() {
        val message = Message(TOPIC_TEST, TestMessageTypes.TEST_NULL_PAYLOAD_REQ, null)
        val response = p2p.send(receiver, message, 1337, _class = Any::class.java)

        assert(response == null) { "send() returned not null response" }
    }

    @Test
    fun `test multiple requests`() {
        val message = Message(TOPIC_TEST, TestMessageTypes.TEST_MULTIPLE_REQUESTS, "some payload")

        repeat(200) {
            val response = p2p.send(receiver, message, 1337, _class = String::class.java)
            assert(response == "some payload")
        }
    }

    @Test
    fun `test no return value on response`() {
        val message = Message(TOPIC_TEST, TestMessageTypes.TEST_NO_RETURN_TYPE_ON_RESPONSE_REQ, "test")
        val v = p2p.send(receiver, message, 1337, _class = String::class.java)

        assert(v == null)
    }
}

fun assertThrows(code: () -> Unit) {
    var threw = false

    try {
        code()
    } catch (e: Exception) {
        threw = true
    }

    assert(threw)
}