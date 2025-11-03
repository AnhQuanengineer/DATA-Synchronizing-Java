package org.datapipeline.Connector;
import org.datapipeline.Config.*;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.Map;

/**
 * Lớp kết nối và quản lý Redis Client (sử dụng Jedis).
 * Triển khai AutoCloseable để mô phỏng Python Context Manager (with... as...).
 */
public class RedisConnect implements AutoCloseable {

    private final String host;
    private final int port;
    private final String user;
    private final String password;
    private final int db;

    private JedisPooled client;

    // Constructor Private: Chỉ được gọi bởi Builder
    private RedisConnect(Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.user = builder.user;
        this.password = builder.password;
        this.db = builder.db;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String host;
        private Integer port;
        private String user;
        private String password;
        private Integer db; // Dùng Integer để có thể kiểm tra null

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder user(String user) {
            this.user = user;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder db(int db) {
            this.db = db;
            return this;
        }

        public RedisConnect build() {
            // Kiểm tra các trường bắt buộc (user/password có thể là null nếu không dùng auth)
            if (host == null || port == null || db == null) {
                throw new IllegalStateException("Redis config requires host, port, and database index (db).");
            }
            return new RedisConnect(this);
        }
    }

    // 
    // STEP 2: connect
    //

    /**
     * Thiết lập kết nối đến Redis và trả về client đã kết nối.
     *
     * @return JedisPooled client đã kết nối.
     * @throws RuntimeException nếu kết nối thất bại.
     */
    public JedisPooled connect() {
        if (this.client != null) {
            return this.client;
        }

        try {
            String uri;
            if (this.password != null && !this.password.isEmpty()) {
                // Bao gồm username và password (nếu có)
                String authPart = (this.user != null && !this.user.isEmpty()) ?
                        this.user + ":" + this.password + "@" :
                        this.password + "@";

                uri = String.format("redis://%s%s:%d/%d",
                        authPart, this.host, this.port, this.db);
            } else {
                // Không có password/auth
                uri = String.format("redis://%s:%d/%d", this.host, this.port, this.db);
            }

            // BƯỚC 2: Khởi tạo JedisPooled bằng URI
            // Constructor này (URI) thường giải quyết vấn đề đơn giản nhất
            this.client = new JedisPooled(uri);

            // Hoặc, nếu URI không hoạt động:
            // this.client = new JedisPooled(this.host, this.port);
            // Sau đó bạn phải cấu hình user/password/db bằng cách tạo JedisPool trước.

            System.out.println("-----------------Connected to Redis-------------------");
            return this.client;

        } catch (JedisConnectionException e) {
            // JedisConnectionException là lỗi chuẩn khi kết nối thất bại
            throw new RuntimeException("-------Failed to connect Redis: " + e.getMessage() + "---------", e);
        }
    }

    //
    // STEP 3: close / disconnect
    //

    /**
     * Đóng pool kết nối Redis.
     */
    public void disconnect() {
        if (this.client != null) {
            // Đóng pool và giải phóng tài nguyên
            this.client.close();
            this.client = null;
            System.out.println("------------Redis connection closed-----------------");
        }
    }

    //
    // STEP 5: exit (AutoCloseable)
    //

    /**
     * Triển khai phương thức close() từ AutoCloseable. Tương đương __exit__.
     */
    @Override
    public void close() {
        this.disconnect();
    }

    //
    // STEP 4: reconnect
    //

    public void reconnect() {
        this.disconnect();
        this.connect();
    }

    // Getter để truy cập client đã kết nối
    public JedisPooled getClient() {
        if (client == null) {
            connect(); // Đảm bảo kết nối nếu chưa có
        }
        return client;
    }

    public static void main(String[] args) {
        RedisConfig redisConfig;

        try {
            // 1. Khởi tạo nguồn cấu hình và Loader
            // Cần đảm bảo có thư viện dotenv-java và file .env đã được setup
            ConfigurationSource source = new EnvironmentSource();
            ConfigLoader loader = new ConfigLoader(source);

            // 2. Tải tất cả cấu hình DB
            Map<String, ValidateConfig> dbConfigMap = loader.getDatabaseConfig();

            // 3. Lấy cấu hình Redis và ép kiểu
            ValidateConfig redisBase = dbConfigMap.get("redis");
            if (redisBase == null) {
                throw new IllegalStateException("Redis config not found in database configurations.");
            }
            redisConfig = (RedisConfig) redisBase;
        } catch (Exception e) {
            System.err.println("LỖI CẤU HÌNH BAN ĐẦU: Không thể tải các biến môi trường.");
            System.err.println("Chi tiết: " + e.getMessage());
            return; // Dừng chương trình nếu không tải được cấu hình
        }

        try(RedisConnect redis = RedisConnect.builder()
                // Dùng các giá trị đã tải từ ConfigLoader
                .host(redisConfig.getHost())
                .port(redisConfig.getPort())
                // Giả định user/password có thể là null trong RedisConfig
                .user(redisConfig.getUser())
                .password(redisConfig.getPassword())
                .db(redisConfig.getDatabase())
                .build()) {
            // Kết nối (Tương đương __enter__ thủ công)
            JedisPooled client = redis.connect();

            // Ví dụ: Set và Get một key
            String key = "app_status";
            client.set(key, "READY");
            String value = client.get(key);

            System.out.println("Status: Connected to Redis DB " + redisConfig.getDatabase());
            System.out.println("Set key '" + key + "' -> Read value: " + value);
        } catch (RuntimeException e) {
            // Bắt lỗi kết nối/thao tác Redis
            System.err.println("LỖI KẾT NỐI/RUNTIME: " + e.getMessage());
        }
    }
}