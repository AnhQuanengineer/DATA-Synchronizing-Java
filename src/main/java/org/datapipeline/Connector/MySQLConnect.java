package org.datapipeline.Connector;

import org.datapipeline.Config.*;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;

/**
 * Lớp kết nối và quản lý MongoDB Session.
 * Triển khai AutoCloseable để mô phỏng Python Context Manager (with... as...).
 */
public class MySQLConnect implements AutoCloseable {

    private final String host;
    private final int port;
    private final String user;
    private final String password;

    private Connection connection;
    private Statement statement;

    // Constructor Private: Chỉ được gọi bởi Builder
    public MySQLConnect(Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.user = builder.user;
        this.password = builder.password;
    }

    // Phương thức tĩnh để bắt đầu Builder
    public static Builder builder() {return new Builder();}

    public static class Builder {
        private String host;
        private Integer port;
        private String user;
        private String password;

        public Builder host(String host) {this.host = host;return this;}
        public Builder port(int port) {this.port = port;return this;}
        public Builder user(String user) {this.user = user;return this;}
        public Builder password(String password) {this.password = password;return this;}

        public MySQLConnect build() {
            if (host == null || port == null || user == null || password == null) {
                throw new IllegalStateException("MySQL config requires host, port, user, and password.");
            }
            return new MySQLConnect(this);
        }
    }

    /**
     * Thiết lập kết nối đến MongoDB và trả về đối tượng MongoDatabase.
     * Tương đương với phương thức connect() trong Python.
     * @return MongoDatabase đã kết nối.
     * @throws RuntimeException nếu kết nối thất bại.
     */
    public void connect() {
        if (this.connection != null && this.statement != null) {
            System.out.println("--------------------Mysql is already connected------------------");
            return;
        }

        String jdbcUrl = String.format("jdbc:mysql://%s:%d/?user=%s&password=%s",
                host, port, user, password);

        try{
            // Bước 1: Thiết lập kết nối
            this.connection = DriverManager.getConnection(jdbcUrl);

            this.connection.setAutoCommit(false);

            // Bước 2: Tạo đối tượng Statement (tương đương cursor)
            this.statement = this.connection.createStatement();

            System.out.println("--------------------Connected to Mysql------------------");
        } catch (SQLException e) {
            // SQLException là lỗi JDBC chuẩn khi kết nối thất bại
            throw new RuntimeException("---------Can't connect to MySql: " + e.getMessage() + "-------------", e);
        }
    }

    public void disconnect() {
        try {
            if (this.statement != null) {
                this.statement.close();
                this.statement = null;
            }
            if (this.connection != null && !this.connection.isClosed()) {
                this.connection.close();
                this.connection = null;
            }
            System.out.println("---------------Mysql connection closed----------------------");
        } catch (SQLException e) {
            System.err.println("Error closing MySQL connection: " + e.getMessage());
        }
    }

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

    public void reconnect(){
        this.disconnect();
        this.connect();
    }

    /**
     * Trả về đối tượng MongoDatabase đã kết nối.
     */
    public Connection getConnection() {
        if (connection == null) connect();
        return this.connection;
    }

    public Statement getStatement() {
        if (statement == null) connect();
        return this.statement;
    }

    public static void main(String[] args) {
        MySQLConfig mySQLConfig;

        try {
            // 1. Khởi tạo nguồn cấu hình và Loader
            // Cần đảm bảo có thư viện dotenv-java và file .env đã được setup
            ConfigurationSource source = new EnvironmentSource();
            ConfigLoader loader = new ConfigLoader(source);

            // 2. Tải tất cả cấu hình DB
            Map<String, DatabaseConfig> dbConfigMap = loader.getDatabaseConfig();

            // 3. Lấy cấu hình MySQL và ép kiểu
            DatabaseConfig mysqlBase = dbConfigMap.get("mysql");
            if (mysqlBase == null) {
                throw new IllegalStateException("MySQL config not found in database configurations.");
            }
            mySQLConfig = (MySQLConfig) mysqlBase;
        } catch (Exception e) {
            System.err.println("LỖI CẤU HÌNH BAN ĐẦU: " + e.getMessage());
            return; // Dừng chương trình nếu không tải được cấu hình
        }

        try (MySQLConnect mysql = MySQLConnect.builder()
                .host(mySQLConfig.getHost())
                .port(mySQLConfig.getPort())
                .user(mySQLConfig.getUser())
                .password(mySQLConfig.getPassword())
                .build()) {
            mysql.connect();

            // Thực hiện truy vấn (ví dụ: tạo Statement/Cursor)
            Statement stmt = mysql.getStatement();
            System.out.println("Status: Statement (Cursor) created successfully.");
            System.out.println("Sẵn sàng thao tác với DB: " + mySQLConfig.getDatabase());
        } catch (RuntimeException e) {
            // Bắt lỗi kết nối/SQL
            System.err.println("LỖI KẾT NỐI/SQL: " + e.getMessage());
        }
    }
}
