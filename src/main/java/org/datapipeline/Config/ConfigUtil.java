package org.datapipeline.Config;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
// Giả định bạn có các lớp cấu hình đã chuyển đổi từ câu hỏi trước:


/**
 * Lớp tiện ích để tạo ra các cấu hình cụ thể cho Spark (ví dụ: JDBC URL).
 */
public class ConfigUtil {

    /**
     * Tạo Map cấu hình Spark từ các cấu hình cơ sở dữ liệu đã tải.
     * @return Map chứa cấu hình cho MySQL, MongoDB, Redis.
     */
    public static Map<String, Object> getSparkConfig() throws IOException {

        // Bước 1: Khởi tạo nguồn cấu hình
        ConfigurationSource source = new EnvironmentSource();
        // Bước 2: Inject nguồn cấu hình vào loader
        ConfigLoader loader = new ConfigLoader(source);

        Map<String, ValidateConfig> dbConfigs = loader.getDatabaseConfig(); // Lấy cấu hình DB từ ConfigLoader

        Map<String, Object> sparkConfig = new HashMap<>();

        // Lấy cấu hình MySQL
        MySQLConfig mysqlConfig = (MySQLConfig) dbConfigs.get("mysql");

        // --- Cấu hình MySQL ---
        Map<String, Object> mysqlMap = new HashMap<>();
        mysqlMap.put("table_users", mysqlConfig.getTableUsers());
        mysqlMap.put("table_repositories", mysqlConfig.getTableRepositories());

        // Tạo JDBC URL
        String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s",
                mysqlConfig.getHost(),
                mysqlConfig.getPort(),
                mysqlConfig.getDatabase());
        mysqlMap.put("jdbc_url", jdbcUrl);

        Map<String, Object> mysqlJdbcConfig = new HashMap<>();
        mysqlJdbcConfig.put("host", mysqlConfig.getHost());
        mysqlJdbcConfig.put("port", mysqlConfig.getPort());
        mysqlJdbcConfig.put("user", mysqlConfig.getUser());
        mysqlJdbcConfig.put("password", mysqlConfig.getPassword());
        mysqlJdbcConfig.put("database", mysqlConfig.getDatabase());

        mysqlMap.put("config", mysqlJdbcConfig);
        sparkConfig.put("mysql", mysqlMap);

        // --- Cấu hình MongoDB ---
        MongoDBConfig mongoConfig = (MongoDBConfig) dbConfigs.get("mongodb");
        Map<String, String> mongoMap = new HashMap<>();
        mongoMap.put("database", mongoConfig.getDbName());
        mongoMap.put("collection", mongoConfig.getCollection());
        mongoMap.put("uri", mongoConfig.getUri());
        sparkConfig.put("mongodb", mongoMap);

        // --- Cấu hình Redis ---
        // Vì cấu hình Redis trong Python gốc trống, ta cũng để trống
        sparkConfig.put("redis", new HashMap<>());

        return sparkConfig;
    }

    public static void main(String[] args) {
        try {
            Map<String, Object> sparkConfig = getSparkConfig();
            System.out.println("Spark Configuration Loaded:");
            // Sử dụng System.out.println() trên Map sẽ in ra chuỗi đại diện
            Map<String,Object> mysql = (Map<String, Object>) sparkConfig.get("mysql");
            System.out.println(mysql);

            // Ví dụ khởi tạo SparkConnect (Giả định):
            /*
            SparkConnect sparkConnect = new SparkConnect(
                "MyDataPipelineApp",
                "local[*]",
                "4g", 2, "2g", 3,
                null, // jars
                (Map<String, String>) sparkConfig.get("mysql"), // spark_conf
                "WARN"
            );
            sparkConnect.stop();
            */

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
