package net.stits.osen.utils

fun assertThrows(code: () -> Unit, lazyMessage: () -> String) {
    try {
        code()
        assert(false) { lazyMessage() }
    } catch (e: Exception) { }
}