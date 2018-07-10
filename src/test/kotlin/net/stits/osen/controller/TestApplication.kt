package net.stits.osen.controller

import net.stits.osen.P2P
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean


@SpringBootApplication
open class TestApplication {
    private val packageToScan = TestApplication::class.java.`package`.name

    @Bean()
    open fun netInitializer(): P2P {
        // default port is 1337, to switch it use "node.port" spring env property
        return P2P(basePackages = arrayOf(packageToScan))
    }
}

fun main(args: Array<String>) {
    SpringApplication.run(TestApplication::class.java, *args)
}