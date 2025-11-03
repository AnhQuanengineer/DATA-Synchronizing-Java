package org.datapipeline.Config;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ConfigLoader {

    // Sử dụng ConfigurationSource đã được inject (DI)
    private final ConfigurationSource source;

    public ConfigLoader(ConfigurationSource source) {
        this.source = source;
    }

    private String getEnv(String key) throws IOException {
        return source.getString(key);
    }

    /**
     * Tải biến môi trường dưới dạng Integer, trả về null nếu giá trị không có
     * hoặc ném ngoại lệ nếu giá trị không phải là số.
     */
    private Integer getIntEnv(String key) throws IOException {
        String value = getEnv(key);
        if (value == null) {
            return null;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Config value for " + key + " is not a valid integer.", e);
        }
    }

    public Map<String, ValidateConfig> getDatabaseConfig() throws IllegalArgumentException, IOException {
        Map<String, ValidateConfig> config = new HashMap<>();

        /*
        Nó thực hiện ba việc chính:

        Thêm một mục cấu hình vào map (config.put).

        Xây dựng đối tượng cấu hình MongoDBConfig bất biến.

        Lấy các giá trị cấu hình từ nguồn bên ngoài (getEnv).
         */
        // 1. Cấu hình MongoDB
        config.put("mongodb", MongoDBConfig.builder()
                .uri(getEnv("MONGO_URI"))
                .dbName(getEnv("MONGO_DB_NAME"))
                .jarPath(getEnv("MONGO_JAR_PATH"))
                .collection(getEnv("MONGO_COLLECTION"))
                .build()
        );

        // 2. Cấu hình MySQL
        Integer mysqlPort = getIntEnv("MYSQL_PORT");
        if (mysqlPort == null) {
            throw new IllegalArgumentException("Missing required config: MYSQL_PORT");
        }

        config.put("mysql", MySQLConfig.builder()
                .host(getEnv("MYSQL_HOST"))
                .port(mysqlPort)
                .user(getEnv("MYSQL_USER"))
                .password(getEnv("MYSQL_PASSWORD"))
                .database(getEnv("MYSQL_DATABASE"))
                .jarPath(getEnv("MYSQL_JAR_PATH"))
                .tableUsers(getEnv("MYSQL_TABLE_USERS"))
                .tableRepositories(getEnv("MYSQL_TABLE_REPOS"))
                .build()
        );

        // 3. Cấu hình Redis
        Integer redisPort = getIntEnv("REDIS_PORT");
        if (redisPort == null) {
            throw new IllegalArgumentException("Missing required config: REDIS_PORT");
        }

        config.put("redis", RedisConfig.builder()
                .host(getEnv("REDIS_HOST"))
                .port(redisPort)
                .user(getEnv("REDIS_USER"))
                .password(getEnv("REDIS_PASSWORD"))
                .database(getIntEnv("REDIS_DB"))
                .jarPath(getEnv("REDIS_JAR_PATH"))
                .build()
        );

        return config;
    }

    public Map<String, ValidateConfig> getKafkaConfig() throws IllegalArgumentException, IOException {
        Map<String, ValidateConfig> config = new HashMap<>();
        KafkaConfig kafkaConfig = new KafkaConfig(getEnv("KAFKA_SERVER"), getEnv("KAFKA_TOPIC"), getEnv("KAFKA_GROUP_ID"));
        /*
        Nó thực hiện ba việc chính:

        Thêm một mục cấu hình vào map (config.put).

        Xây dựng đối tượng cấu hình KafkaConfig bất biến.

        Lấy các giá trị cấu hình từ nguồn bên ngoài (getEnv).
         */

        // Cấu hình Kafka
        config.put("kafka", kafkaConfig);
        return config;
    }

    // Hàm main để chạy thử nghiệm
    public static void main(String[] args) {
        try {
            // Bước 1: Khởi tạo nguồn cấu hình,đọc env
            ConfigurationSource source = new EnvironmentSource();
            // Bước 2: Inject nguồn cấu hình vào loader,tạo thành dict
            ConfigLoader loader = new ConfigLoader(source);

            // Bước 3: Tải cấu hình,tạo thành dict
            Map<String, ValidateConfig> dbConfig = loader.getDatabaseConfig();

            System.out.println("Tải cấu hình thành công!");
            System.out.println("-------------------------");

            MySQLConfig mysqlConfig = (MySQLConfig) dbConfig.get("mysql");
            System.out.println("MySQL Host: " + mysqlConfig.getHost());
            System.out.println("MySQL Port: " + mysqlConfig.getPort());

//            System.out.println(((MySQLConfig) dbConfig.get("mysql")).getDatabase());

        } catch (IllegalStateException | IllegalArgumentException e) {
            System.err.println("LỖI CẤU HÌNH: " + e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Đã xảy ra lỗi không mong muốn:");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
