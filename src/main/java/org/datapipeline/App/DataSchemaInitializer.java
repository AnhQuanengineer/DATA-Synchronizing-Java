package org.datapipeline.App;

import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import org.datapipeline.Connector.MongoDBConnect;
import org.datapipeline.Connector.MySQLConnect;
import org.datapipeline.Connector.RedisConnect;
import org.datapipeline.Utils.SchemaManager;

import org.datapipeline.Config.ConfigLoader;
import org.datapipeline.Config.ConfigurationSource;
import org.datapipeline.Config.EnvironmentSource;
import org.datapipeline.Config.ValidateConfig;
import org.datapipeline.Config.MongoDBConfig;
import org.datapipeline.Config.MySQLConfig;
import org.datapipeline.Config.RedisConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Map;

/**
 * Lớp chính để tải cấu hình, khởi tạo kết nối và thiết lập schema dữ liệu.
 */
public class DataSchemaInitializer {

    /*
    Việc khai báo Map<String, DatabaseConfig> ConfigMap; cho phép bạn lưu trữ cả
    ba loại cấu hình (MongoDBConfig, MySQLConfig, RedisConfig) vào cùng một Map, miễn là cả
    ba lớp này đều kế thừa từ lớp DatabaseConfig.
    Trong Lập trình Hướng đối tượng (OOP),
    một biến có kiểu Lớp Cha (DatabaseConfig) có thể chứa một tham chiếu đến bất kỳ đối tượng nào là Lớp Con của nó
     */
    // Hàm thực hiện logic chính (tương đương def main(Config):)
    public static void initializeSchemas(Map<String, ValidateConfig> ConfigMap) {

        // Lấy cấu hình cụ thể
        MongoDBConfig mongoConfig = (MongoDBConfig) ConfigMap.get("mongodb");
        /*
        tương đương
        **DatabaseConfig mongoGeneric = ConfigMap.get("mongodb"); // Trả về DatabaseConfig
        **MongoDBConfig mongoConfig = (MongoDBConfig) mongoGeneric; // Ép kiểu để lấy lại các thuộc tính/phương thức riêng của MongoDBConfig
         */
        MySQLConfig mysqlConfig = (MySQLConfig) ConfigMap.get("mysql");
        RedisConfig redisConfig = (RedisConfig) ConfigMap.get("redis");

        // ------------------------------------------------------------------
        // MONGODB
        // ------------------------------------------------------------------
        System.out.println("\n--- Bắt đầu xử lý MongoDB ---");
        try (MongoDBConnect mongoClient = MongoDBConnect.builder()
                .uri(mongoConfig.getUri())
                .dbName(mongoConfig.getDbName())
                .build()) {

            // 1. Kết nối và lấy database
            MongoDatabase db = mongoClient.connect();

            // 2. Tạo Schema và Validation
            SchemaManager.createMongoDB_Schema(db);

            // 3. Thêm dữ liệu mẫu
            Document userData = new Document()
                    .append("user_id", 1)
                    .append("login", "GoogleCodeExporter")
                    .append("gravatar_id", "")
                    .append("avatar_url", "https://avatars.githubusercontent.com/u/9614759?")
                    .append("url", "https://api.github.com/users/GoogleCodeExporter");

            db.getCollection(mongoConfig.getCollection()).insertOne(userData);
            System.out.println("------------Inserted to MongoDB----------------");

            // 4. Validate Schema
            SchemaManager.validateMongoDB_Schema(db);

        } catch (Exception e) {
            System.err.println("LỖI XỬ LÝ MONGODB: " + e.getMessage());
        } // mongoClient.close() được gọi tự động

        // ------------------------------------------------------------------
        // MYSQL
        // ------------------------------------------------------------------
        System.out.println("\n--- Bắt đầu xử lý MySQL ---");
        try (MySQLConnect mysqlClient = MySQLConnect.builder()
                .host(mysqlConfig.getHost())
                .port(mysqlConfig.getPort())
                .user(mysqlConfig.getUser())
                .password(mysqlConfig.getPassword())
                .build()) {

            // 1. Kết nối (sẽ được gọi trong get/create)
            Connection connection = mysqlClient.getConnection();

            // 2. Tạo Schema
            SchemaManager.createMySQL_Schema(connection); // Hàm này tự động USE/COMMIT

            // 3. Thêm dữ liệu mẫu (sử dụng PreparedStatement để tránh SQL Injection)
            String sql = "INSERT INTO Users (user_id, login, gravatar_id, avatar_url, url) VALUES (?, ?, ?, ?, ?)";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setInt(1, 1);
                statement.setString(2, "GoogleCodeExporter");
                statement.setString(3, "");
                statement.setString(4, "https://avatars.githubusercontent.com/u/9614759?");
                statement.setString(5, "https://api.github.com/users/GoogleCodeExporter");

                statement.executeUpdate();
                connection.commit();
                System.out.println("------------Inserted data to My SQL---------------------");
            }

            // 4. Validate Schema
            SchemaManager.validateMySQL_Schema(connection);

        } catch (Exception e) {
            System.err.println("LỖI XỬ LÝ MYSQL: " + e.getMessage());
        } // mysqlClient.close() được gọi tự động

        // ------------------------------------------------------------------
        // REDIS
        // ------------------------------------------------------------------
        System.out.println("\n--- Bắt đầu xử lý Redis ---");
        try (RedisConnect redisClient = RedisConnect.builder()
                .host(redisConfig.getHost())
                .port(redisConfig.getPort())
                .user(redisConfig.getUser())
                .password(redisConfig.getPassword())
                .db(redisConfig.getDatabase())
                .build()) {

            // 1. Kết nối và lấy client
            redisClient.connect();

            // 2. Tạo Schema (bao gồm thêm dữ liệu mẫu và validate)
            SchemaManager.createRedis_Schema(redisClient.getClient());
            SchemaManager.validateRedis_Schema(redisClient.getClient());

        } catch (Exception e) {
            System.err.println("LỖI XỬ LÝ REDIS: " + e.getMessage());
        } // redisClient.close() được gọi tự động
    }

    // Tương đương if __name__ == "__main__":
    public static void main(String[] args) {
        System.out.println(">>> KHỞI TẠO SCHEMA DỮ LIỆU <<<");

        // BƯỚC 1: Tải cấu hình từ môi trường
        Map<String, ValidateConfig> Config;
        try {
            ConfigurationSource source = new EnvironmentSource();
            ConfigLoader loader = new ConfigLoader(source);
            Config = loader.getDatabaseConfig();
        } catch (Exception e) {
            System.err.println("LỖI KHÔNG THỂ TẢI CẤU HÌNH: Dừng ứng dụng.");
            System.err.println("Chi tiết: " + e.getMessage());
            return;
        }

        // BƯỚC 2: Khởi tạo schema với cấu hình đã tải
        initializeSchemas(Config);

        System.out.println("\n>>> HOÀN TẤT QUÁ TRÌNH KHỞI TẠO SCHEMA <<<");
    }
}
