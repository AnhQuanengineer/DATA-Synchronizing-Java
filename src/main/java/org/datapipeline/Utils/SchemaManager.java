package org.datapipeline.Utils;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.client.model.CreateCollectionOptions; // Cần thiết để tạo collection với validation
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.Protocol; // Cần thiết cho sendCommand
import org.bson.BsonDocument;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.nio.file.Paths;
import java.util.HashMap;

public class SchemaManager {

    private static final Path SQL_FILE_PATH = Paths.get("/home/victo/Anhquan-Spark-Java/DATA-Synchronizing/DATA-Synchronizing/src/main/java/org/datapipeline/Sql");
    private static final String DEFAULT_DATABASE_NAME = "github_data";

    //
    // --- MONGODB SCHEMA MANAGEMENT (ĐÃ SỬA createCollection) ---
    //

    /**
     * Tạo schema MongoDB với JSON Schema Validation.
     * @param db Đối tượng MongoDatabase đã kết nối.
     */
    public static void createMongoDB_Schema(MongoDatabase db) {
        String userSchema =
                "{ $jsonSchema: { bsonType: 'object', required: ['user_id', 'login'], properties: {" +
                        "  user_id: { bsonType: 'int' }," +
                        "  login: { bsonType: 'string' }," +
                        "  gravatar_id: { bsonType: ['string', 'null'] }," +
                        "  avatar_url: { bsonType: ['string', 'null'] }," +
                        "  url: { bsonType: ['string', 'null'] }" +
                        "} } }";

        String repoSchema =
                "{ $jsonSchema: { bsonType: 'object', required: ['repo_id', 'name'], properties: {" +
                        "  repo_id: { bsonType: 'int' }," +
                        "  name: { bsonType: 'string' }," +
                        "  url: { bsonType: ['string', 'null'] }" +
                        "} } }";

        // Logic tạo Collection Users
        ValidationOptions userValidation = new ValidationOptions().validator(BsonDocument.parse(userSchema));
        // SỬA: Dùng CreateCollectionOptions để truyền ValidationOptions
        CreateCollectionOptions userOptions = new CreateCollectionOptions().validationOptions(userValidation);
        try { db.getCollection("Users").drop(); } catch (Exception ignored) {}
        db.createCollection("Users", userOptions);

        // Logic tạo Collection Repositories
        ValidationOptions repoValidation = new ValidationOptions().validator(BsonDocument.parse(repoSchema));
        // SỬA: Dùng CreateCollectionOptions để truyền ValidationOptions
        CreateCollectionOptions repoOptions = new CreateCollectionOptions().validationOptions(repoValidation);
        try { db.getCollection("Repositories").drop(); } catch (Exception ignored) {}
        db.createCollection("Repositories", repoOptions);

        System.out.println("------------SCHEMA CREATED IN MONGODB---------------------");
    }

    /**
     * Xác thực sự tồn tại của các Collection trong MongoDB.
     * @param db Đối tượng MongoDatabase đã kết nối.
     */
    public static void validateMongoDB_Schema(MongoDatabase db) {
        List<String> collections = db.listCollectionNames().into(new java.util.ArrayList<>());

        if (!collections.contains("Users") || !collections.contains("Repositories")) {
            throw new IllegalArgumentException("---------------------Missing collection in MongoDB-----------------------");
        }

        if (db.getCollection("Users").find(new org.bson.Document("user_id", 1)).first() == null) {
            throw new IllegalArgumentException("---------------------user_id not found in MongoDB-----------------------");
        }

        System.out.println("---------------Validated schema in MongoDB----------------------");
    }

    //
    // --- MYSQL SCHEMA MANAGEMENT (Giữ nguyên) ---
    //

    /**
     * Tạo database và thực thi script SQL để tạo bảng trong MySQL.
     * @param connection Đối tượng Connection đã kết nối (chưa chọn database).
     * @throws SQLException nếu có lỗi SQL.
     * @throws IOException nếu lỗi đọc file.
     */
    public static void createMySQL_Schema(Connection connection) throws SQLException, IOException {
        try (Statement cursor = connection.createStatement()) {

            // 1. Tạo Database
            cursor.executeUpdate("DROP DATABASE IF EXISTS " + DEFAULT_DATABASE_NAME);
            cursor.executeUpdate("CREATE DATABASE IF NOT EXISTS " + DEFAULT_DATABASE_NAME);
            connection.commit();
            System.out.println(String.format("===========Created database %s in MySQL!================", DEFAULT_DATABASE_NAME));

            // 2. Chuyển sang database mới và thực thi script
            cursor.executeUpdate("USE " + DEFAULT_DATABASE_NAME);

            // Đọc và thực thi file SQL
            String sqlScript = readSqlFile();
            String[] sqlCommands = sqlScript.split(";");

            for (String cmd : sqlCommands) {
                String trimmedCmd = cmd.trim();
                if (!trimmedCmd.isEmpty()) {
                    cursor.executeUpdate(trimmedCmd);
                    System.out.println(String.format("----------------Execute: %s...---------------", trimmedCmd.substring(0, Math.min(trimmedCmd.length(), 50))));
                }
            }
            connection.commit();
            System.out.println("---------Created Mysql Schema-----------------");

        } catch (SQLException e) {
            connection.rollback();
            throw new SQLException(String.format("Can not execute to sql command: %s----------------------", e.getMessage()), e);
        }
    }

    private static String readSqlFile() throws IOException {
        // Đặt tên file/đường dẫn resource (ví dụ: nằm trong src/main/resources/schema.sql)
        String resourceName = "sql/schema.sql";

        try (InputStream is = SchemaManager.class.getClassLoader().getResourceAsStream(resourceName);
             BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {

            if (is == null) {
                throw new IOException("Resource file not found: " + resourceName);
            }

            return reader.lines().collect(Collectors.joining("\n"));

        } catch (NullPointerException | IOException e) {
            throw new IOException("Failed to read SQL resource file: " + resourceName, e);
        }
    }

    /**
     * Xác thực sự tồn tại của các bảng trong MySQL.
     * @param connection Đối tượng Connection đã kết nối (phải trỏ đến database mục tiêu).
     * @throws IllegalArgumentException nếu thiếu bảng hoặc dữ liệu mẫu không tồn tại.
     */
    public static void validateMySQL_Schema(Connection connection) throws SQLException {
        try (Statement cursor = connection.createStatement()) {

            // 1. Lấy danh sách bảng
            cursor.execute("SHOW TABLES");
            List<String> tables = new java.util.ArrayList<>();
            try (ResultSet rs = cursor.getResultSet()) {
                while (rs.next()) {
                    tables.add(rs.getString(1));
                }
            }

            System.out.println("haha" + tables);

            if (!tables.contains("Users") || !tables.contains("Repositories")) {
                throw new IllegalArgumentException("---------------------Missing tables in MySQL-----------------------");
            }

            // 2. Kiểm tra dữ liệu mẫu
            cursor.execute("SELECT * FROM Users WHERE user_id = 1");
            if (!cursor.getResultSet().next()) {
                throw new IllegalArgumentException("---------User not found-----------------");
            }

            System.out.println("---------------Validated schema in MySQL----------------------");
        }
    }

    //
    // --- REDIS SCHEMA MANAGEMENT (ĐÃ SỬA sendCommand) ---
    //

    /**
     * Xóa database và thêm dữ liệu mẫu vào Redis.
     * @param client JedisPooled client đã kết nối.
     * @throws RuntimeException nếu thất bại.
     */
    public static void createRedis_Schema(JedisPooled client) {
        try {
            // SỬA: Truyền mảng byte rỗng (new byte[0]) để giải quyết lỗi Ambiguous
            client.sendCommand(new byte[0], Protocol.Command.FLUSHDB);

            Map<String, String> userData = new HashMap<>();
            userData.put("login", "GoogleCodeExporter");
            userData.put("gravatar_id", "");
            userData.put("avatar_url", "https://avatars.githubusercontent.com/u/9614759?");
            userData.put("url", "https://api.githubusercontent.com/users/GoogleCodeExporter");

            client.hset("user:1", userData);

            client.sadd("user_id", "user:1");

            System.out.println("--------------------Add data to Redis successfully---------------");

        } catch (Exception e) {
            throw new RuntimeException(String.format("----------Failed to add data to Redis: %s------------", e.getMessage()), e);
        }
    }

    /**
     * Xác thực dữ liệu mẫu trong Redis.
     * @param client JedisPooled client đã kết nối.
     * @throws IllegalArgumentException nếu dữ liệu mẫu không chính xác.
     */
    public static void validateRedis_Schema(JedisPooled client) {
        Map<String, String> userData = client.hgetAll("user:1");

        if (!"GoogleCodeExporter".equals(userData.get("login"))) {
            throw new IllegalArgumentException("--------Value login not found or incorrect in Redis------------");
        }

        if (!client.sismember("user_id", "user:1")) {
            throw new IllegalArgumentException("-----------User not set in Redis---------------");
        }

        System.out.println("------------Validated schema in Redis-----------------");
    }
}