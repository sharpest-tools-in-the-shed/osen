package net.stits.osen.flow

import net.stits.osen.*


@P2PFlow
class TestFlow {

    @FlowInitiator
    fun first(param: Int, counterParty: Address): Flow<Int> {
        println("Sent $param to $counterParty")

        return Flow.next(TestFlow::second, param)
    }

    fun second(counterParty: Address, param: Int): Flow<Int> {
        println("Received $param from $counterParty, sending back...")

        return Flow.next(TestFlow::third, param)
    }

    @FlowFinalizer
    fun third(param: Int, recipient: Address) {
        println("Received $param back from $recipient")
    }
}
