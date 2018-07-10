package net.stits.osen.controller

import kotlinx.coroutines.experimental.runBlocking
import net.stits.osen.Address
import net.stits.osen.Message
import net.stits.osen.P2P
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner


/**
 * Sometimes tests may fail because of UDP not sending messages
 */
@RunWith(SpringJUnit4ClassRunner::class)
@SpringBootTest
class SpringP2PAppTest {
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
            runBlocking {
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