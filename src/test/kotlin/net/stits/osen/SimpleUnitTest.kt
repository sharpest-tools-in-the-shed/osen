package net.stits.osen

import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import net.stits.osen.controller.TOPIC_TEST
import net.stits.osen.controller.TestMessageTypes
import net.stits.osen.controller.TestPayload
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner
import java.util.*


/**
 * Sometimes tests may fail because of UDP not sending messages
 */
@RunWith(SpringJUnit4ClassRunner::class)
@SpringBootTest
class SimpleUnitTest {
    @Autowired
    lateinit var p2p: P2P
    private val receiver = Address("localhost", 1337)

    @Test
    fun `test single request and response`() = runBlocking {
        val message = Message(
            TOPIC_TEST,
            TestMessageTypes.TEST,
            TestPayload()
        )
        val response = p2p.sendAndReceive<String>(receiver, message)

        assert(response == "Test string payload") { "send() returned invalid response" }
    }

    @Test
    fun `test bytearray payload`() = runBlocking {
        val payload = ByteArray(30) { it.toByte() }
        val message = Message(
            TOPIC_TEST,
            TestMessageTypes.TEST_BYTEARRAY_PAYLOAD, payload)

        val response = p2p.sendAndReceive<ByteArray>(receiver, message)

        assert(response.contentEquals(payload)) { "send() returned invalid response" }
    }

    private fun generateHugePayload(): String {
        val rng = Random()
        val res = ByteArray(3000) { rng.nextInt().toByte() }

        return Base64.getEncoder().encodeToString(res)
    }

    @Test
    fun `test multiple requests with huge payload`() = runBlocking {
        val payload = generateHugePayload()
        val message = Message(TOPIC_TEST, TestMessageTypes.TEST_MULTIPLE_REQUESTS, payload)

        repeat(200) {
            val response = p2p.sendAndReceive<String>(receiver, message)
            assert(response == payload)
            println("Request #$it")
        }
    }

    @Test
    fun `test simple send`() = runBlocking {
        val message = Message(
            TOPIC_TEST,
            TestMessageTypes.TEST_SIMPLE_SEND, "test")
        p2p.send(receiver, message)
        // this delay is needed for controller to process sent message
        delay(3000)
    }
}