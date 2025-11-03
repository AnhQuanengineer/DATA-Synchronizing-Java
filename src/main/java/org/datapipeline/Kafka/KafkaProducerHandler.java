package org.datapipeline.Kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datapipeline.Config.KafkaConfig;
import org.datapipeline.Config.ValidateConfig;
import org.datapipeline.Connector.MySQLConnect;
import org.datapipeline.Models.MessageRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaProducerHandler {
    private static final Logger LOG = LogManager.getLogger(KafkaProducerHandler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final KafkaProducer<String, String> producer;
    private final DataTrigger dataTrigger;
    private final List<MessageRecord> buffer = new ArrayList<>();
    private long count = 0;

    public KafkaProducerHandler(Map<String, ValidateConfig> ConfigKafka) {
        this.producer = createProducer(ConfigKafka);
        this.dataTrigger = new DataTrigger();
    }

    private KafkaProducer<String, String> createProducer(Map<String, ValidateConfig> ConfigKafka) {
        KafkaConfig kafkaConfig = (KafkaConfig) ConfigKafka.get("kafka");

        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaConfig.getBootstrapServers());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("enable.idempotence", "true");
        return new KafkaProducer<>(props);
    }

    public List<MessageRecord> produce(String lastTimestamp, MySQLConnect mySQLConnect, Map<String, ValidateConfig> ConfigKafka) throws Exception {
        KafkaConfig kafkaConfig = (KafkaConfig) ConfigKafka.get("kafka");
        DataTrigger.TriggerResult result = dataTrigger.getDataTrigger(mySQLConnect, lastTimestamp);
        List<MessageRecord> sent = new ArrayList<>();

        for (Map<String, Object> record : result.data) {
            count++;
            MessageRecord messageRecord = new MessageRecord(count, record);
            String json = MAPPER.writeValueAsString(record);
            producer.send(new ProducerRecord<>(kafkaConfig.getTopic(), json));
            sent.add(messageRecord);
            buffer.add(messageRecord);
            LOG.info("PRODUCER [{}]: {}", count, record);
        }

        if (!sent.isEmpty()) {
            producer.flush();
            LOG.info("PRODUCER → Đã gửi {} bản ghi", sent.size());
        }

        if (buffer.size() > 1000) {
            buffer.subList(0, buffer.size() - 1000).clear();
        }

        return sent;
    }

    public List<MessageRecord> getBuffer() {
        return new ArrayList<>(buffer);
    }

    public KafkaProducer<String, String> getProducer() {
        return producer;
    }

    public void close() {
        if (producer != null) {
            producer.close();
        }
    }
}
