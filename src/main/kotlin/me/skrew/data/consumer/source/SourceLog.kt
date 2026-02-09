package me.skrew.data.consumer.source

import java.time.LocalDateTime

data class SourceLog (
    val summary: String,
    val createdAt: LocalDateTime,
)