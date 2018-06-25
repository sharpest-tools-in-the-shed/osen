package net.stits.osen

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication


@SpringBootApplication
open class Application


fun main(args: Array<String>) {
    drawBanner()

    SpringApplication.run(Application::class.java, *args)
}