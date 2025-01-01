package com.nextwebspark.doc_chunk_pipeline.services;

import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
public class DocumentService {

    private final S3Service s3Service;
    private final KafkaProducerService kafkaProducerService;

    @Value("${chunk.size}")
    private int chunkSize;

    public DocumentService(S3Service s3Service, KafkaProducerService kafkaProducerService) {
        this.s3Service = s3Service;
        this.kafkaProducerService = kafkaProducerService;
    }

    public void processAndUpload(MultipartFile file) throws Exception {
        // Step 1: Upload file to S3
        String s3Url = s3Service.uploadFile(file);

        // Step 2: Extract content from file
        String content = extractContent(file);

        // Step 3: Split content into chunks
        List<String> chunks = splitContent(content, chunkSize);

        // Step 4: Publish chunks to Kafka
        String documentId = UUID.randomUUID().toString();
        for (int i = 0; i < chunks.size(); i++) {
            kafkaProducerService.sendChunk(documentId, i, chunks.get(i));
        }
    }

    private String extractContent(MultipartFile file) throws IOException, TikaException {
        try (InputStream inputStream = file.getInputStream()) {
            return new Tika().parseToString(inputStream);
        }
    }

    private List<String> splitContent(String content, int size) {
        List<String> chunks = new ArrayList<>();
        for (int start = 0; start < content.length(); start += size) {
            chunks.add(content.substring(start, Math.min(content.length(), start + size)));
        }
        return chunks;
    }
}

