package org.datapipeline.Kafka;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datapipeline.Config.*;
import org.datapipeline.Connector.MySQLConnect;
import org.datapipeline.Models.MessageRecord;

import java.util.List;
import java.util.Map;

public class KafkaTriggerMain {
    // Giữ nguyên logger context theo yêu cầu của bạn
    private static final Logger LOG = LogManager.getLogger(KafkaProducerHandler.class);

    public static void main(String[] args) {
        Map<String, ValidateConfig> ConfigKafka = loadKafkaConfig();

        Map<String, ValidateConfig> databaseConfig = loadDatabaseConfig();

        if (databaseConfig == null) {
            LOG.error("Không tải được cấu hình Database → DỪNG ỨNG DỤNG");
            return;
        }

        if (ConfigKafka == null) {
            LOG.error("Không tải được cấu hình Kafka → DỪNG ỨNG DỤNG");
            return;
        }

        // 2. KHỞI TẠO CÁC HANDLER
        // LỖI ĐÃ SỬA: mySQLConnect (Connection Pool) được tạo MỘT LẦN ở đây
        MySQLConnect mySQLConnect = createMySQLConnect(databaseConfig);

        // Kiểm tra mySQLConnect trước khi tiếp tục
        if (mySQLConnect == null) {
            LOG.error("Không thể khởi tạo MySQL Connect (Connection Pool) → DỪNG ỨNG DỤNG");
            return;
        }

        KafkaProducerHandler kafkaProducerHandler = new KafkaProducerHandler(ConfigKafka);
        KafkaConsumerHandler kafkaConsumerHandler = new KafkaConsumerHandler(ConfigKafka);

        String lastTimestamp = null;

        LOG.info("=== KAFKA TRIGGER (FULL - JAVA 8) ĐÃ KHỞI ĐỘNG ===");

        // 3. VÒNG LẶP CHÍNH
        try {
            while (true) {
                // SỬ DỤNG mySQLConnect (Pool) đã được khởi tạo

                // PRODUCER: Đọc MySQL → Gửi Kafka → Lưu buffer với count
                List<MessageRecord> sent = kafkaProducerHandler.produce(lastTimestamp, mySQLConnect, ConfigKafka);
                if (!sent.isEmpty()) {
                    lastTimestamp = getLastTimestamp(sent);
                }

                // CONSUMER: Đọc Kafka → Tạo MessageRecord với count
                List<MessageRecord> received = kafkaConsumerHandler.consume();

                // VALIDATE: So sánh count → Gửi lại nếu thiếu
                if (!received.isEmpty() || !sent.isEmpty()) {
                    DataValidator.validateAndResend(
                            sent
                            , received
                            , kafkaProducerHandler.getProducer()
                    );
                }

                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Thread bị interrupt → thoát");
        } catch (Exception e) {
            LOG.error("Lỗi nghiêm trọng trong vòng lặp chính", e);
        } finally {
            // 4. DỌN DẸP TÀI NGUYÊN
            LOG.info("Đang dừng ứng dụng...");
            kafkaConsumerHandler.close();
            kafkaProducerHandler.close();

            // ĐÓNG CONNECTION POOL (mySQLConnect.close() sẽ đóng Pool HikariCP)
            if (mySQLConnect != null) {
                try { mySQLConnect.close(); } catch (Exception ignored) {}
            }
            LOG.info("Ứng dụng đã dừng hoàn toàn");
        }
    }

    public static Map<String, ValidateConfig> loadKafkaConfig() {
        try {
            ConfigurationSource source = new EnvironmentSource();
            ConfigLoader loader = new ConfigLoader(source);
            return loader.getKafkaConfig();
        } catch (Exception e) {
            System.err.println("LỖI KHÔNG THỂ TẢI CẤU HÌNH: Dừng ứng dụng.");
            System.err.println("Chi tiết: " + e.getMessage());
            return null;
        }
    }

    public static Map<String, ValidateConfig> loadDatabaseConfig() {
        try {
            ConfigurationSource source = new EnvironmentSource();
            ConfigLoader loader = new ConfigLoader(source);
            // SỬA LỖI LOGIC: Phải tải cấu hình Database
            return loader.getDatabaseConfig();
        } catch (Exception e) {
            System.err.println("LỖI KHÔNG THỂ TẢI CẤU HÌNH: Dừng ứng dụng.");
            System.err.println("Chi tiết: " + e.getMessage());
            return null;
        }
    }

    public static MySQLConnect createMySQLConnect(Map<String, ValidateConfig> config) {
        MySQLConfig mysqlConfig = (MySQLConfig) config.get("mysql");
        if (mysqlConfig == null) {
            LOG.error("KHÔNG TÌM THẤY CẤU HÌNH MYSQL");
            return null;
        }
        return MySQLConnect.builder()
                .host(mysqlConfig.getHost())
                .port(mysqlConfig.getPort())
                .user(mysqlConfig.getUser())
                .password(mysqlConfig.getPassword())
                .build();
    }

    public static String getLastTimestamp(List<MessageRecord> records) {
        if (records == null || records.isEmpty()) return null;
        String max = null;

        for (MessageRecord record : records) {
            String ts = (String) record.getData().get("log_timestamp");
            if (max == null || (ts != null && ts.compareTo(max) > 0)) {
                max = ts;
            }
        }
        return max;
    }
}
