package com.nextwebspark.doc_chunk_pipeline.services;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.topic.name}")
    private String topicName;

    public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendChunk(String documentId, int chunkIndex, String chunkContent) {
        String message = String.format("{\"documentId\": \"%s\", \"chunkIndex\": %d, \"content\": \"%s\"}",
                documentId, chunkIndex, chunkContent);
        kafkaTemplate.send(topicName, documentId, message);
    }
}
