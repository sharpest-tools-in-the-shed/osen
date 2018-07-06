package net.stits.osen

import kotlinx.coroutines.experimental.async
import kotlinx.coroutines.experimental.runBlocking
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
    fun `test single request and response`() = runBlocking {
        val response = p2p.requestFrom<String>(receiver) {
            Message(TOPIC_TEST, TestMessageTypes.TEST, TestPayload())
        }

        assert(response == "Test string payload") { "send() returned invalid response" }
    }

    @Test
    fun `test null payload`() = runBlocking {
        val response = p2p.requestFrom<String>(receiver) {
            Message(TOPIC_TEST, TestMessageTypes.TEST_NULL_PAYLOAD, null)
        }

        assert(response == null) { "send() returned not null response" }
    }

    @Test
    fun `test multiple requests`() {
        val message = Message(TOPIC_TEST, TestMessageTypes.TEST_MULTIPLE_REQUESTS, "some payload")

        repeat(200) {
            async {
                val response = p2p.requestFrom<String>(receiver) { message }
                assert(response == "some payload")
            }
        }
    }

    @Test
    fun `test simple send`() {
        p2p.sendTo(receiver) {
            Message(TOPIC_TEST, TestMessageTypes.TEST_SIMPLE_SEND, "test")
        }
    }
}

fun assertThrows(code: () -> Unit) {
    try {
        code()
        assert(false)
    } catch (e: Exception) { }
}