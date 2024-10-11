package me.skrew.data.consumer.es;

import lombok.Builder;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;

import java.time.LocalDateTime;

@Document(indexName = "logs")
@Builder
public class Log {

    @Id
    private Long id;
    private String summary;
    private LocalDateTime createdAt;

}
