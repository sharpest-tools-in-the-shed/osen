package net.stits.osen

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean


@SpringBootApplication
open class Application {
    private val port = 1337
    private val packageToScan = "net.stits"

    @Bean
    open fun p2pInitializer(): P2P {
        return P2P(listeningPort = port, packageToScan = packageToScan)
    }
}


fun main(args: Array<String>) {
    SpringApplication.run(Application::class.java, *args)
}