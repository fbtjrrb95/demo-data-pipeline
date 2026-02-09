package me.skrew.data.consumer

import me.skrew.data.consumer.es.Log
import me.skrew.data.consumer.es.LogRepository
import me.skrew.data.consumer.source.EventPayload
import mu.KotlinLogging
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class StoreESConsumer(
    private val logRepository: LogRepository,
) {
    private val logger = KotlinLogging.logger {}

    @KafkaListener(topics = ["source.source.log"])
    fun consume(@Payload data: EventPayload) {
        try {
            val after = data.payload.after
            logger.info { "received data$after" }
            val log = Log(
                summary = after.summary,
                createdAt = after.createdAt,
            )
            logRepository.save(log)
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }
}
