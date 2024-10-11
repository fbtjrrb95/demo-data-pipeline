package me.skrew.data.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import me.skrew.data.consumer.es.Log;
import me.skrew.data.consumer.es.LogRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class StoreESConsumer {

    private final ObjectMapper objectMapper;
    private final LogRepository logRepository;

    @KafkaListener(topics = "source.source.log")
    public void consume(@Payload ConsumerRecord<String, String> data) {
        try {
            Map<String, Map<String, Object>> map = objectMapper.readValue(data.value(), new TypeReference<>() {});
            Map<String, Object> payload = map.get("payload");

            Log log = buildLog(payload);
            logRepository.save(log);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Log buildLog(Map<String, Object> payload) {
        return Log.builder()
                .summary((String) payload.get("summary"))
                .createdAt(LocalDateTime.now())
                .build();
    }
}
