package me.skrew.data.consumer.source

data class EventPayload(
    val payload: Payload
) {
    data class Payload(
        val after: SourceLog
    )
}