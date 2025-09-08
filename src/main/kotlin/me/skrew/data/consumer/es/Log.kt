package me.skrew.data.consumer.es

import org.springframework.data.annotation.Id
import org.springframework.data.elasticsearch.annotations.Document
import java.time.LocalDateTime

@Document(indexName = "logs")
class Log(
    private val summary: String,
    private val createdAt: LocalDateTime
) {
    @Id
    private val id: String? = null
}
