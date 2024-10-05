package me.skrew.data.consumer;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class StoreToESConsumer {

    private final ObjectMapper objectMapper;

    public StoreToESConsumer() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    @KafkaListener(topics = "source.source.log")
    public void consume(@Payload ConsumerRecord<String, String> data) {
        try {
            Map<String, Map<String, Object>> map = objectMapper.readValue(data.value(), new TypeReference<>() {});
            Map<String, Object> payload = map.get("payload");
            System.out.println("Received data: " + payload);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
