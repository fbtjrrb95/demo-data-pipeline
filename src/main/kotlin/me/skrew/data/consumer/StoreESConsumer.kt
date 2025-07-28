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
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class StoreESConsumer {

    private final ObjectMapper objectMapper;
    private final LogRepository logRepository;
    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneId.of("UTC"));

    @SuppressWarnings("unchecked")
    @KafkaListener(topics = "source.source.log")
    public void consume(@Payload ConsumerRecord<String, String> data) {
        try {
            Map<String, Map<String, Object>> map = objectMapper.readValue(data.value(), new TypeReference<>() {});
            Map<String, Object> payload = map.get("payload");

            Map<String, String> after = (Map<String, String>) payload.get("after");
            Log log = buildLog(after);
            logRepository.save(log);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Log buildLog(Map<String, String> data) {
        return Log.builder()
                .summary(data.get("summary"))
                .createdAt(LocalDateTime.parse(data.get("createdAt"), formatter))
                .build();
    }
}
