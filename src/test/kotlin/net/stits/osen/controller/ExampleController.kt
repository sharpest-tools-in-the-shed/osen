package net.stits.osen.controller

import net.stits.osen.*
import org.springframework.stereotype.Service


const val TOPIC_TEST = "TEST"
object TestMessageTypes {
    const val TEST = "TEST"
    const val TEST_NULL_PAYLOAD = "TEST_NULL_PAYLOAD"
    const val TEST_MULTIPLE_REQUESTS = "TEST_MULTIPLE_REQUESTS"
    const val TEST_SIMPLE_SEND = "TEST_SIMPLE_SEND"
}

data class TestPayload(val text: String = "test")

/**
 * Example controller
 *
 * You can autowire everything spring allows to autowire
 */
@P2PController(TOPIC_TEST)
class ExampleController(private val service: TestService) {
    /**
     * You can write handlers in any way you like:
     *      handleTestRequest()
     *      handleTestRequest(a: TestPayload)
     *      handleTestRequest(b: Address)
     *      handleTestRequest(x: TestPayload, y: Address)
     *      handleTestRequest(sender: Address, payload: TestPayload)
     * just make sure you passing to it no more than 2 parameters, sender is always Address (payload can be Any?)
     */
    @OnRequest(TestMessageTypes.TEST)
    fun `test single request`(payload: TestPayload, sender: Address): String {
        assert(payload.text.isNotEmpty()) { "Payload is invalid" }
        assert(sender.host.isNotEmpty()) { "Sender host is unknown" }
        assert(sender.port == 1337) { "Sender port is not 1337" }

        return "Test string payload"
    }

    @OnResponse(TestMessageTypes.TEST)
    fun `test single response`(payload: String): String {
        assert(service.doSomething(payload)) { "Autowire doesn't work" }

        return payload
    }

    @OnRequest(TestMessageTypes.TEST_NULL_PAYLOAD)
    fun `test null payload request`(payload: String?): String? {
        assert(payload == null)

        return payload
    }

    @OnResponse(TestMessageTypes.TEST_NULL_PAYLOAD)
    fun `test null payload response`(payload: String?): String? {
        assert(payload == null)

        return payload
    }

    @OnRequest(TestMessageTypes.TEST_MULTIPLE_REQUESTS)
    fun `test multiple requests`(payload: String): String {
        return payload
    }

    @OnResponse(TestMessageTypes.TEST_MULTIPLE_REQUESTS)
    fun `test multiple responses`(payload: String): String {
        return payload
    }

    @On(TestMessageTypes.TEST_SIMPLE_SEND)
    fun `test simple send`(payload: String) {
        assert(payload == "test")
    }
}

@Service
class TestService {
    fun doSomething(value: String): Boolean {
        assert(value == "Test string payload") { "Payload not equal" }
        return true
    }
}