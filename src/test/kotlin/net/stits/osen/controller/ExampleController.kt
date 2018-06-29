package net.stits.osen.controller

import net.stits.osen.*
import org.springframework.stereotype.Service


const val TOPIC_TEST = "TEST"
object TestMessageTypes {
    const val TEST_REQUEST = "TEST_REQ"
    const val TEST_RESPONSE = "TEST_RES"
    const val TEST_NULL_PAYLOAD_REQ = "TEST_NULL_PAYLOAD_REQ"
    const val TEST_NULL_PAYLOAD_RES = "TEST_NULL_PAYLOAD_RES"
    const val TEST_MULTIPLE_REQUESTS = "TEST_MULTIPLE_REQUESTS"
    const val TEST_MULTIPLE_RESPONSES = "TEST_MULTIPLE_RESPONSES"
    const val TEST_NO_RETURN_TYPE_ON_RESPONSE_REQ = "TEST_NO_RETURN_TYPE_ON_RESPONSE_REQ"
    const val TEST_NO_RETURN_TYPE_ON_RESPONSE_RES = "TEST_NO_RETURN_TYPE_ON_RESPONSE_RES"
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
     *      handleTestRequest(x: TestPayload, y: Address, z: Session)
     *      handleTestRequest(sender: Address, payload: TestPayload)
     * just make sure you passing to it no more than 3 parameters, sender is always Address and session is always Session (payload can be Any?)
     */
    @On(TestMessageTypes.TEST_REQUEST)
    fun `test single request`(payload: TestPayload, sender: Address, session: Session) {
        assert(payload.text.isNotEmpty()) { "Payload is invalid" }
        assert(sender.host.isNotEmpty()) { "Sender host is unknown" }
        assert(sender.port == 1337) { "Sender port is not 1337" }
        assert(session.getStage() == SessionStage.REQUEST) { "Session stage is invalid" }
        assert(session.id >= 0) { "Session id is invalid" }

        val message = Message(TOPIC_TEST, TestMessageTypes.TEST_RESPONSE, "Test string payload")
        P2P.send(recipient = sender, message = message, listeningPort = 1337, _session = session)
    }

    @On(TestMessageTypes.TEST_RESPONSE)
    fun `test single response`(payload: String): String {
        assert(service.doSomething(payload)) { "Autowire doesn't work" }

        return payload
    }

    @On(TestMessageTypes.TEST_NULL_PAYLOAD_REQ)
    fun `test null payload request`(payload: Any?, sender: Address, session: Session) {
        assert(payload == null)

        val message = Message(TOPIC_TEST, TestMessageTypes.TEST_NULL_PAYLOAD_RES, payload)
        P2P.send(sender, message, 1337, _session = session)
    }

    @On(TestMessageTypes.TEST_NULL_PAYLOAD_RES)
    fun `test null payload response`(payload: Any?): Any? {
        assert(payload == null)

        return payload
    }

    @On(TestMessageTypes.TEST_MULTIPLE_REQUESTS)
    fun `test multiple requests`(payload: String, sender: Address, session: Session) {
        val message = Message(TOPIC_TEST, TestMessageTypes.TEST_MULTIPLE_RESPONSES, payload)
        P2P.send(sender, message, 1337, _session = session)
    }

    @On(TestMessageTypes.TEST_MULTIPLE_RESPONSES)
    fun `test multiple responses`(payload: String): String {
        return payload
    }

    @On(TestMessageTypes.TEST_NO_RETURN_TYPE_ON_RESPONSE_REQ)
    fun `test no return type on response req`(payload: String, sender: Address, session: Session) {
        val message = Message(TOPIC_TEST, TestMessageTypes.TEST_NO_RETURN_TYPE_ON_RESPONSE_RES, payload)
        P2P.send(sender, message, 1337, _session = session)
    }

    @On(TestMessageTypes.TEST_NO_RETURN_TYPE_ON_RESPONSE_RES)
    fun `test no return type on response res`(payload: String) {}
}

@Service
class TestService {
    fun doSomething(value: String): Boolean {
        assert(value == "Test string payload") { "Payload not equal" }
        return true
    }
}