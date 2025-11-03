package org.datapipeline.Kafka;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datapipeline.Config.*;
import org.datapipeline.Connector.MySQLConnect;
import org.datapipeline.Spark.SparkWriteData;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DataTrigger {
    private static final Logger LOG = LogManager.getLogger(SparkWriteData.class);
    private static final String DEFAULT_DATABASE_NAME = "github_data";
    private static final String TOPIC = "quandz";

    // Class kết quả
    public static class TriggerResult {
        public List<Map<String, Object>> data;
        public String newTimestamp;

        public TriggerResult(List<Map<String, Object>> data, String newTimestamp) {
            this.data = data;
            this.newTimestamp = newTimestamp;
        }
    }

    /*
     * Lấy dữ liệu từ user_log_after theo last_timestamp
     * @param mysqlClient Kết nối MySQL
     * @param lastTimestamp Timestamp cũ (format: YYYY-MM-DD HH:MI:SS.ffffff)
     * @return TriggerResult chứa data + newTimestamp
     */

    public TriggerResult getDataTrigger(MySQLConnect mysqlClient, String lastTimestamp) {
        List<Map<String, Object>> data = new ArrayList<>();
        String newTimestamp = lastTimestamp != null ? lastTimestamp : "";

        String sql = "SELECT user_id, login, gravatar_id, avatar_url, url, state, " +
                "DATE_FORMAT(log_timestamp, '%Y-%m-%d %H:%i:%s.%f') AS log_timestamp1 " +
                "FROM user_log_after";

        StringBuilder queryBuilder = new StringBuilder(sql);
        boolean hasWhere = false;

        if (lastTimestamp != null && !lastTimestamp.trim().isEmpty()) {
            queryBuilder.append(" WHERE DATE_FORMAT(log_timestamp, '%Y-%m-%d %H:%i:%s.%f') > ?");
            hasWhere = true;
        }

        String finalQuery = queryBuilder.toString();

        try (Connection connection = mysqlClient.getConnection()) {

            try (Statement stmt = connection.createStatement()) {
                stmt.executeUpdate("USE " + DEFAULT_DATABASE_NAME);
            }

            try (PreparedStatement pstmt = connection.prepareStatement(finalQuery)) {
                if (hasWhere) {
                    pstmt.setString(1, lastTimestamp);
                }

                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        Map<String, Object> row = new LinkedHashMap<>();
                        row.put("user_id", rs.getObject("user_id"));
                        row.put("login", rs.getString("login"));
                        row.put("gravatar_id", rs.getString("gravatar_id"));
                        row.put("avatar_url", rs.getString("avatar_url"));
                        row.put("url", rs.getString("url"));
                        row.put("state", rs.getString("state"));
                        row.put("log_timestamp", rs.getString("log_timestamp1"));
                        data.add(row);
                    }
                }
            }
            // Tính newTimestamp
            if (!data.isEmpty()) {
                newTimestamp = data.stream()
                        .map(m -> (String) m.get("log_timestamp"))
                        .max(String::compareTo)
                        .orElse(lastTimestamp);
            }

            LOG.info("Đã đọc {} bản ghi từ user_log_after. newTimestamp = {}", data.size(), newTimestamp);
        } catch (SQLException e) {
            LOG.error("LỖI SQL KHI LẤY DỮ LIỆU TỪ user_log_after", e);
        } catch (Exception e) {
            LOG.error("LỖI HỆ THỐNG KHI LẤY DỮ LIỆU", e);
        }
        return new TriggerResult(data, newTimestamp);
    }
    public static void main(String[] args) {

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
        MySQLConfig mysqlConfig = (MySQLConfig) Config.get("mysql");

        MySQLConnect mysqlClient = MySQLConnect.builder()
                .host(mysqlConfig.getHost())
                .port(mysqlConfig.getPort())
                .user(mysqlConfig.getUser())
                .password(mysqlConfig.getPassword())
                .build();

        DataTrigger trigger = new DataTrigger();
        TriggerResult result = trigger.getDataTrigger(mysqlClient, null);

        System.out.println("Số bản ghi: " + result.data);
        System.out.println("Timestamp mới: " + result.newTimestamp);
    }
}


