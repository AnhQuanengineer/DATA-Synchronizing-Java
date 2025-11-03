package org.datapipeline.Connector;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

// Bỏ các import không cần thiết (DriverManager)

/**
 * Lớp quản lý Connection Pool cho MySQL bằng HikariCP.
 * Triển khai AutoCloseable để đóng Pool khi ứng dụng tắt.
 */
public class MySQLConnect implements AutoCloseable {

    // Sử dụng static volatile để đảm bảo Pool là Singleton và thread-safe
    private static volatile HikariDataSource dataSource = null;
    private static final Object lock = new Object(); // Đối tượng khóa cho đồng bộ hóa

    private final String host;
    private final int port;
    private final String user;
    private final String password;

    // Loại bỏ biến 'connection' và 'statement' instance

    // Constructor Private: Chỉ được gọi bởi Builder
    public MySQLConnect(Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.user = builder.user;
        this.password = builder.password;
    }

    // Phương thức tĩnh để bắt đầu Builder
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String host;
        private Integer port;
        private String user;
        private String password;

        public Builder host(String host) { this.host = host; return this; }
        public Builder port(int port) { this.port = port; return this; }
        public Builder user(String user) { this.user = user; return this; }
        public Builder password(String password) { this.password = password; return this; }

        public MySQLConnect build() {
            if (host == null || port == null || user == null || password == null) {
                throw new IllegalStateException("MySQL config requires host, port, user, and password.");
            }
            MySQLConnect instance = new MySQLConnect(this);
            instance.initializePool(); // Khởi tạo Pool khi đối tượng được xây dựng lần đầu
            return instance;
        }
    }

    // Logic khởi tạo Pool (Chỉ chạy một lần duy nhất)
    private void initializePool() {
        if (dataSource == null) {
            synchronized (lock) {
                if (dataSource == null) {
                    HikariConfig config = new HikariConfig();

                    // LƯU Ý QUAN TRỌNG: Nếu bạn đã xác định database (ví dụ: github_data)
                    // trong DataTrigger, hãy thêm nó vào URL để loại bỏ lệnh USE.
                    // Ví dụ: String jdbcUrl = String.format("jdbc:mysql://%s:%d/github_data", host, port);
                    String jdbcUrl = String.format("jdbc:mysql://%s:%d", host, port);

                    config.setJdbcUrl(jdbcUrl);
                    config.setUsername(user);
                    config.setPassword(password);

                    // Cấu hình Pool cơ bản để tối ưu cho ứng dụng chạy dài
                    config.setMinimumIdle(5); // Số lượng kết nối nhàn rỗi tối thiểu
                    config.setMaximumPoolSize(20); // Số lượng kết nối tối đa
                    config.setConnectionTimeout(30000); // 30 giây chờ lấy kết nối
                    config.setIdleTimeout(600000); // 10 phút timeout cho kết nối nhàn rỗi
                    config.setMaxLifetime(1800000); // 30 phút là thời gian sống tối đa của một kết nối

                    config.setAutoCommit(false);

                    dataSource = new HikariDataSource(config);
                    System.out.println("--------------------MySQL Connection Pool (HikariCP) Initialized------------------");
                }
            }
        }
    }

    /**
     * Lấy một kết nối từ Pool. Kết nối này sẽ được trả về Pool
     * khi phương thức close() được gọi trên nó (thường là qua try-with-resources).
     * @return Connection từ Pool.
     */
    public Connection getConnection() throws SQLException {
        if (dataSource == null) {
            // Đây là lỗi nghiêm trọng, Pool chưa được khởi tạo
            throw new SQLException("Connection Pool has not been initialized. Call build() first in your main method.");
        }
        // Kết nối được mượn từ Pool
        return dataSource.getConnection();
    }

    // Phương thức này giờ đây là không an toàn trong môi trường Pooling và bị loại bỏ.
    // Các Statement nên được tạo bên trong khối try-with-resources của hàm gọi.
    public Statement getStatement() {
        System.err.println("WARNING: getStatement() is deprecated in Connection Pooling environment. Use getConnection().createStatement() inside a try-with-resources block.");
        return null;
    }

    // Các phương thức sau bị loại bỏ/thay đổi chức năng vì đã có Pooling:

    // connect() giờ đây chỉ là một placeholder
    public void connect() {
        System.out.println("--------------------Pool is already initialized. Call getConnection() to borrow a Connection------------------");
    }

    // disconnect() giờ đây là một placeholder
    public void disconnect() {
        System.out.println("--------------------Call close() to shut down the entire Pool when exiting the application----------------------");
    }

    // reconnect() giờ đây là một placeholder
    public void reconnect(){
        System.out.println("--------------------Pool manages reconnections automatically----------------------");
    }

    /**
     * Triển khai close() từ AutoCloseable.
     * Phương thức này đóng toàn bộ Connection Pool (CHỈ KHI ỨNG DỤNG TẮT HẲN).
     */
    @Override
    public void close() {
        if (dataSource != null) {
            dataSource.close();
            dataSource = null;
            System.out.println("--------------------MySQL Connection Pool Shut Down Successfully----------------------");
        }
    }
}
