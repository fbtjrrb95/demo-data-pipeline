package me.skrew.data.consumer

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.kotlinModule
import me.skrew.data.consumer.source.EventPayload
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.support.serializer.JsonDeserializer

@EnableKafka
@Configuration
class KafkaConsumerConfiguration(
    @Value("\${kafka.bootstrap-servers}")
    private val bootstrapServers: String
) {
    @Bean
    fun consumerFactory(): ConsumerFactory<String, EventPayload> {
        val objectMapper = ObjectMapper()
            .registerModule(JavaTimeModule())
            .registerModule(kotlinModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

        val value = JsonDeserializer(EventPayload::class.java, objectMapper)
        value.addTrustedPackages("*")
        value.setUseTypeHeaders(false)

        val config: Map<String, Any> = mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ConsumerConfig.GROUP_ID_CONFIG to "store-es",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to JsonDeserializer::class.java,
        )
        return DefaultKafkaConsumerFactory(config, StringDeserializer(), value)
    }

    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, EventPayload> {
        val factory = ConcurrentKafkaListenerContainerFactory<String, EventPayload>()
        factory.consumerFactory = consumerFactory()
        return factory
    }
}
