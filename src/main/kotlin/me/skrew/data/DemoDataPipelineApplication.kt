package me.skrew.data

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class DemoDataPipelineApplication

fun main(args: Array<String>) {
    runApplication<DemoDataPipelineApplication>(*args)
}