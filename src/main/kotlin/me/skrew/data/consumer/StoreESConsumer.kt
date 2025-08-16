package me.skrew.data.consumer

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import me.skrew.data.consumer.es.Log
import me.skrew.data.consumer.es.LogRepository
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter


@Component
class StoreESConsumer(

    private val logRepository: LogRepository,
) {
    private val objectMapper = jacksonObjectMapper()
    private val logger = KotlinLogging.logger {}

    private val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneId.of("UTC"))

    @KafkaListener(topics = ["source.source.log"])
    fun consume(@Payload data: ConsumerRecord<String?, String?>) {
        try {
            val map: Map<String, Map<String, Any>> = objectMapper.readValue(data.value(), object : TypeReference<Map<String, Map<String, Any>>>() {})
            val payload = map["payload"]!!
            val after = payload["after"] as Map<String, String>?
            val log = buildLog(after!!)
            logger.info { "received data$log" }
            logRepository.save<Log>(log)
        } catch (e: java.lang.Exception) {
            throw java.lang.RuntimeException(e)
        }
    }

    private fun buildLog(data: Map<String, String>): Log {
        return Log(
            summary = data["summary"],
            createdAt = LocalDateTime.parse(data["createdAt"], formatter),
        )
    }
}
