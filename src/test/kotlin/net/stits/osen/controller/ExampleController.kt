package net.stits.osen.controller

import net.stits.osen.*
import org.springframework.stereotype.Service
import javax.annotation.PostConstruct


const val TOPIC_TEST = "TEST"
object TestMessageTypes {
    const val TEST = "TEST"
}

data class TestPayload(val text: String = "test")

/**
 * Example controller
 *
 * You can autowire everything spring allows to autowire
 */
@P2PController(TOPIC_TEST)
class ExampleController(private val service: ExampleService) {

    @PostConstruct
    fun checkIfAutowireWorks() {
        service.doSomething()
    }

    /**
     * You can write handlers in any way you like:
     *      handleTestRequest()
     *      handleTestRequest(a: TestPayload)
     *      handleTestRequest(b: Address)
     *      handleTestRequest(x: TestPayload, y: Address)
     *      handleTestRequest(sender: Address, payload: TestPayload)
     * just make sure you passing to it no more than 2 parameters, and sender is always Address (payload can be Any?)
     */
    @OnRequest(TestMessageTypes.TEST)
    fun handleTestRequest(payload: TestPayload, sender: Address, session: Session) {
        println("RECEIVED TEST REQUEST WITH PAYLOAD $payload, SENDING TEST RESPONSE BACK...")

        val message = Message(TOPIC_TEST, TestMessageTypes.TEST, "Test string payload")
        P2P.sendResponse(sender, message, 1337, session)
    }

    @OnResponse(TestMessageTypes.TEST)
    fun handleTestResponse(payload: String, sender: Address): String {
        println("RECEIVED PONG REQUEST WITH PAYLOAD $payload - THAT HOST IS ALIVE")
        return payload
    }
}

@Service
class ExampleService {
    fun doSomething() {
        println("I did something")
    }
}