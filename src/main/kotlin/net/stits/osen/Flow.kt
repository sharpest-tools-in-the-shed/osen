package net.stits.osen

import java.lang.reflect.Method
import kotlin.reflect.KFunction

data class Flow<T : Any?>(val next: String, val payload: T? = null, val _class: Class<T>) {
    companion object {
        inline fun <reified T : Any?> next(next: KFunction<*>, payload: T? = null): Flow<T> {
            return Flow(next.name, payload, T::class.java)
        }
    }
}

@Target(AnnotationTarget.CLASS)
annotation class P2PFlow

@Target(AnnotationTarget.FUNCTION)
annotation class FlowInitiator

@Target(AnnotationTarget.FUNCTION)
annotation class FlowFinalizer


fun <T> checkMethodHasOnlyOneArgumentWithSpecifiedTypeAndValidReturnValue(className: String, method: Method, payload: T?, payloadClass: Class<T>) {
    val methodName = method.name

    if (payload != null && method.parameters.isEmpty())
        throw RuntimeException("Method $methodName of class $className has invalid signature: you provided a payload but it consumes no parameters.")

    if (payload != null && method.parameters.size > 1)
        throw RuntimeException("Method $methodName of class $className has invalid signature: it should consume only 1 parameter.")

    if (payload == null && method.parameters.isNotEmpty())
        throw RuntimeException("No payload provided for method $methodName of class $className.")

    if (payload != null && payloadClass.isAssignableFrom(method.parameters[0]::class.java))
        throw RuntimeException("Method $methodName of class $className has invalid signature: you provided a payload type: ${payloadClass.canonicalName} but it consumes ${method.parameters[0]::class.java}.")

    if (!Flow::class.java.isAssignableFrom(method.returnType))
        throw RuntimeException("Method $methodName of class $className has invalid signature: it should return an instance of Flow [Flow.checkpoint()]")
}