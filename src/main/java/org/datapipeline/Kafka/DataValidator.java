package org.datapipeline.Kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datapipeline.Config.*;
import org.datapipeline.Models.MessageRecord;

import java.util.*;

public class DataValidator {
    private static final Logger LOG = LogManager.getLogger(DataValidator.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static void resend(MessageRecord messageRecord, KafkaProducer<String, String> producer, Map<String, ValidateConfig> ConfigKafka) {
        KafkaConfig kafkaConfig = (KafkaConfig) ConfigKafka.get("kafka");
        try {
            String json = MAPPER.writeValueAsString(messageRecord.getData());
            producer.send(new ProducerRecord<>(kafkaConfig.getTopic(), json));
            LOG.warn("RESEND [{}]: {}", messageRecord.getCount(), messageRecord.getData());
        } catch (Exception e){
            LOG.error("Lỗi resend {}", e);
        }
    }

    public static void validateAndResend(
            List<MessageRecord> producerData,
            List<MessageRecord> consumerData,
            KafkaProducer<String, String> producer
    ) {

        Map<String, ValidateConfig> Config;
        try {
            ConfigurationSource source = new EnvironmentSource();
            ConfigLoader loader = new ConfigLoader(source);
            Config = loader.getKafkaConfig();
        } catch (Exception e) {
            System.err.println("LỖI KHÔNG THỂ TẢI CẤU HÌNH: Dừng ứng dụng.");
            System.err.println("Chi tiết: " + e.getMessage());
            return;
        }

        if (producerData.isEmpty()) return;

        Set<Long> producerCounts = new HashSet<>();
        Set<Long> consumerCounts = new HashSet<>();
        Map<Long, MessageRecord> producerRecordMap = new HashMap<>();

        for (MessageRecord messageRecord : producerData) {
            producerCounts.add(messageRecord.getCount());
            producerRecordMap.put(messageRecord.getCount(), messageRecord);
        }

        for (MessageRecord messageRecord : consumerData) {
            consumerCounts.add(messageRecord.getCount());
        }

//        boolean hasMissing = false;
//        for(MessageRecord messageRecordProducer : producerData) {
//            if(!consumerCounts.contains(messageRecordProducer.getCount())) {
//                hasMissing = true;
//                resend(messageRecordProducer, producer, Config);
//            }
//        }

        producerCounts.removeAll(consumerCounts);
        boolean hasMissing;

        if (!producerCounts.isEmpty()) {
            hasMissing = true;
            for (Long missingCount : producerCounts) {
                MessageRecord messageRecordProducer = producerRecordMap.get(missingCount);
                if (messageRecordProducer != null) {
                    resend(messageRecordProducer, producer, Config);
                }
            }
        } else {
            hasMissing = false;
        }

        if (hasMissing) {
            LOG.warn("VALIDATE → Thiếu dữ liệu (count không khớp)");
        } else {
            LOG.info("-----VALIDATE PASS----- (count khớp: {})", consumerCounts.size());
        }
    }
}
