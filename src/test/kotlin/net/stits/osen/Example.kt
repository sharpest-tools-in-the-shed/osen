package net.stits.osen

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean


@SpringBootApplication
open class FirstApplication {
    private val packageToScan = FirstApplication::class.java.`package`.name

    @Bean()
    open fun netInitializer(): P2P {
        // default port is 1337, to switch it use "node.port" spring env property
        return P2P(packageToScan = packageToScan)
    }
}

fun main(args: Array<String>) {
    SpringApplication.run(FirstApplication::class.java, *args)
}