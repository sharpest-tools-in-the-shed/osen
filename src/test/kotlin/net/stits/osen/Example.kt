package net.stits.osen

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean


@SpringBootApplication
open class FirstApplication {
    private val port = 1337
    private val packageToScan = "net.stits"

    @Bean()
    open fun netInitializer(): P2P {
        return P2P(listeningPort = port, packageToScan = packageToScan)
    }
}

@SpringBootApplication
open class SecondApplication {
    private val port = 1338
    private val packageToScan = "net.stits"

    @Bean()
    open fun netInitializer(): P2P {
        return P2P(listeningPort = port, packageToScan = packageToScan)
    }
}

fun main(args: Array<String>) {
    SpringApplication.run(FirstApplication::class.java, *args)
}