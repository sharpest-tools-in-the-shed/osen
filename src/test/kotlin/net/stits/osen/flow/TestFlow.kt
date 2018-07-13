package net.stits.osen.flow

import net.stits.osen.Address
import java.lang.reflect.Method
import kotlin.reflect.KFunction


data class FlowCheckpoint<T : Any?>(val next: Method, val payload: T? = null, val _class: Class<T>) {
    companion object {
        inline fun <reified T : Any?> create(next: Method, payload: T? = null): FlowCheckpoint<T> {
            return FlowCheckpoint(next, payload, T::class.java)
        }
    }
}

abstract class P2PFlow {
    // maybe somehow get rid of T
    protected inline fun <reified T : Any?> next(methodReference: KFunction<*>, payload: T? = null) {
        val javaClassInstance = this::class.java
        val methodName = methodReference.name

        val method = javaClassInstance.methods.associateBy { it.name }[methodName]
                ?: throw RuntimeException("Method $methodName does not exists in class ${javaClassInstance.canonicalName}")

        if (payload != null && method.parameters.isEmpty())
            throw RuntimeException("Method $methodName of class ${javaClassInstance.canonicalName} has invalid signature: you provided a payload but it consumes no parameters.")

        if (payload != null && method.parameters.size > 1)
            throw RuntimeException("Method $methodName of class ${javaClassInstance.canonicalName} has invalid signature: it should consume only 1 parameter.")

        if (payload == null && method.parameters.isNotEmpty())
            throw RuntimeException("No payload provided for method $methodName of class ${javaClassInstance.canonicalName}.")

        if (payload != null && T::class.java.isAssignableFrom(method.parameters[0]::class.java))
            throw RuntimeException("Method $methodName of class ${javaClassInstance.canonicalName} has invalid signature: you provided a payload type: ${T::class.java.canonicalName} but it consumes ${method.parameters[0]::class.java}.")

        _nextFlowCheckpoint = FlowCheckpoint.create(method, payload)
    }

    var _nextFlowCheckpoint: FlowCheckpoint<*>? = null
}

@Target(AnnotationTarget.FUNCTION)
annotation class FlowInitiator

class TestFlow : P2PFlow() {

    @FlowInitiator
    fun first(param: Any) {

    }
}

inline fun <reified T, reified P : Any?> startFlow(payload: P?, counterParty: Address) {
    val flowInitiatorMethod = T::class.java.methods.firstOrNull { it.isAnnotationPresent(FlowInitiator::class.java) }
            ?: throw RuntimeException("No method annotated with @FlowInitiator in class ${T::class.java.canonicalName}")

    // check parameters present
}

fun main(args: Array<String>) {
    startFlow<TestFlow, Int>()
}