package org.datapipeline.Connector;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.MongoTimeoutException;
import com.mongodb.MongoSecurityException;
import org.datapipeline.Config.*;

import java.util.Map;

/**
 * Lớp kết nối và quản lý MongoDB Session.
 * Triển khai AutoCloseable để mô phỏng Python Context Manager (with... as...).
 */
public class MongoDBConnect implements AutoCloseable {

    private final String mongoUri;
    private final String dbName;
    private MongoClient client;
    private MongoDatabase database;

    // Constructor Private: Chỉ được gọi bởi Builder
    private MongoDBConnect(Builder builder) {
        this.mongoUri = builder.mongoUri;
        this.dbName = builder.dbName;
    }

    // Phương thức tĩnh để bắt đầu Builder
    public static Builder builder() {
        return new Builder();
    }

    //
    // STEP 1: Connect
    //

    /**
     * Thiết lập kết nối đến MongoDB và trả về đối tượng MongoDatabase.
     * Tương đương với phương thức connect() trong Python.
     * @return MongoDatabase đã kết nối.
     * @throws RuntimeException nếu kết nối thất bại.
     */
    public MongoDatabase connect() {
        if (this.database != null) {
            return this.database; // Trả về kết nối hiện có nếu đã kết nối
        }

        try {
            // Sử dụng MongoClients.create(uri) để khởi tạo client
            this.client = MongoClients.create(this.mongoUri);

            // Test kết nối bằng cách gọi server_info (sẽ ném ra ngoại lệ nếu thất bại)
            this.client.listDatabaseNames().first();

            this.database = this.client.getDatabase(this.dbName);
            System.out.println("--------------------Connected to MongoDB: " + this.dbName + " ------------------");
            return this.database;
        } catch (MongoTimeoutException e) {
            throw new RuntimeException("--------------------Failed to connect MongoDB (Timeout): " + e.getMessage() + "-------------------", e);
        } catch (MongoSecurityException e) {
            throw new RuntimeException("--------------------Failed to connect MongoDB (Authentication/Security): " + e.getMessage() + "-------------------", e);
        } catch (Exception e) {
            throw new RuntimeException("--------------------Failed to connect MongoDB: " + e.getMessage() + "-------------------", e);
        }
    }

    /**
     * Trả về đối tượng MongoDatabase đã kết nối.
     */
    public MongoDatabase getDatabase() {
        if (this.database == null) {
            throw new IllegalStateException("MongoDB is not connected. Call connect() first.");
        }
        return this.database;
    }

    //
    // STEP 2: Disconnect (close)
    //

    /**
     * Đóng kết nối MongoDB Client.
     * Tương đương với phương thức close() trong Python.
     */
    public void disconnect() {
        if (this.client != null) {
            this.client.close();
            this.client = null;
            this.database = null;
            System.out.println("---------------MongoDB connection closed----------------------");
        }
    }

    //
    // STEP 3 & 4: Reconnect & Exit (Sử dụng AutoCloseable)
    //

    // Triển khai phương thức close() từ AutoCloseable.
    // Cho phép sử dụng try-with-resources.
    // Tương đương với __exit__ (tự động đóng khi thoát khối try).
    // Tương đương với __enter__ (connect) được gọi thủ công trước try-with-resources.
    // LƯU Ý: Nếu muốn connect tự động, hãy gọi connect() trong constructor.
    // Ở đây, ta giữ connect() riêng để khớp với thiết kế Python gốc.
    @Override
    public void close() {
        this.disconnect();
    }

    // Phương thức Reconnect
    public void reconnect() {
        this.close();
        this.connect();
    }

    //
    // BUILDER CLASS
    //

    public static class Builder {
        private String mongoUri;
        private String dbName;

        public Builder uri(String uri) { this.mongoUri = uri; return this; }
        public Builder dbName(String name) { this.dbName = name; return this; }

        public MongoDBConnect build() {
            if (mongoUri == null || dbName == null) {
                throw new IllegalStateException("MongoDB URI and Database Name are required.");
            }
            return new MongoDBConnect(this);
        }
    }

    //
    // Ví dụ sử dụng
    //
    public static void main(String[] args) {

        // ------------------------------------------------------------------
        // BƯỚC 1: TẢI CẤU HÌNH TỪ CONFIGLOADER (Mô phỏng lấy từ .env)
        // ------------------------------------------------------------------
        MongoDBConfig mongoConfig;

        try {
            // 1. Khởi tạo nguồn cấu hình và Loader
            ConfigurationSource source = new EnvironmentSource();
            ConfigLoader loader = new ConfigLoader(source);

            // 2. Tải tất cả cấu hình DB
            Map<String, DatabaseConfig> dbConfigMap = loader.getDatabaseConfig();

            // 3. Lấy cấu hình MongoDB và ép kiểu
            DatabaseConfig mongoBase = dbConfigMap.get("mongodb");
            if (mongoBase == null) {
                throw new IllegalStateException("MongoDB config not found in database configurations.");
            }
            mongoConfig = (MongoDBConfig) mongoBase;

        } catch (Exception e) {
            System.err.println("LỖI CẤU HÌNH BAN ĐẦU: " + e.getMessage());
            return; // Dừng chương trình nếu không tải được cấu hình
        }

        // ------------------------------------------------------------------
        // BƯỚC 2: SỬ DỤNG CẤU HÌNH ĐỂ KẾT NỐI
        // ------------------------------------------------------------------

        try (MongoDBConnect mongo = MongoDBConnect.builder()
                // Dùng các giá trị đã tải từ ConfigLoader
                .uri(mongoConfig.getUri())
                .dbName(mongoConfig.getDbName())
                .build()) {

            // Kết nối (Context Manager __enter__)
            MongoDatabase db = mongo.connect();

            System.out.println("Status: Connected, accessing collections in " + mongoConfig.getDbName() + "...");

            // Ví dụ: Liệt kê các collection (thao tác MongoDB)
            db.listCollectionNames().forEach(name -> System.out.println("Collection: " + name));

        } catch (RuntimeException e) {
            // Bắt lỗi kết nối/thao tác MongoDB
            System.err.println("LỖI KẾT NỐI/RUNTIME: " + e.getMessage());
        }
        // Lệnh mongo.close() được gọi tự động (Context Manager __exit__)
    }
}
