package net.stits.osen

fun assertThrows(code: () -> Unit, lazyMessage: () -> String) {
    try {
        code()
        assert(false) { lazyMessage() }
    } catch (e: Exception) { }
}