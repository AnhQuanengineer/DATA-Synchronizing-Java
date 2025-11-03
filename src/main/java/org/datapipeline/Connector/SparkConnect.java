package org.datapipeline.Connector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Lớp này quản lý việc tạo và dừng SparkSession, tương đương với lớp SparkConnect trong Python.
 */
public class SparkConnect {
    private static final Logger LOG = LogManager.getLogger(SparkConnect.class);

    private final String appName;
    private SparkSession spark;

    // Sử dụng Builder Pattern để tạo đối tượng SparkConnect phức tạp hơn,
    // nhưng ở đây ta dùng Constructor đơn giản và truyền tất cả tham số cần thiết.
    public SparkConnect(
            String appName,
            String masterUrl,
            String executorMemory,
            int executorCores,
            String driverMemory,
            int numExecutors,
            List<String> jars,
            Map<String, String> sparkConf,
            String logLevel) {

        this.appName = appName;
        // Khởi tạo SparkSession ngay trong constructor
        this.spark = createSparkSession(
                masterUrl,
                executorMemory,
                executorCores,
                driverMemory,
                numExecutors,
                jars,
                sparkConf,
                logLevel
        );
    }

    /**
     * Phương thức chính để tạo SparkSession với cấu hình tùy chỉnh.
     * * @param masterUrl URL master của Spark (ví dụ: "local[*]")
     * @param executorMemory Bộ nhớ cho mỗi executor
     * @param executorCores Số lõi cho mỗi executor
     * @param driverMemory Bộ nhớ cho driver
     * @param numExecutors Số lượng executor
     * @param jars Danh sách các đường dẫn JAR cần thêm vào classpath
     * @param sparkConf Map chứa các cấu hình Spark tùy chỉnh
     * @param logLevel Mức độ log (ví dụ: "INFO", "WARN")
     * @return SparkSession
     */
    public SparkSession createSparkSession(
            String masterUrl,
            String executorMemory,
            int executorCores,
            String driverMemory,
            int numExecutors,
            List<String> jars,
            Map<String, String> sparkConf,
            String logLevel) {

        SparkSession.Builder builder = SparkSession.builder()
                .appName(this.appName)
                .master(masterUrl);

        // --- Cấu hình tài nguyên ---
        // Trong Java, chúng ta phải kiểm tra null cho các tham số đối tượng, nhưng
        // các tham số cơ bản (String, int) ta giả định đã có giá trị mặc định.

        // Chỉ thêm cấu hình nếu giá trị hợp lý (không phải rỗng)
        if (executorMemory != null && !executorMemory.isEmpty()) {
            builder.config("spark.executor.memory", executorMemory);
        }
        if (executorCores > 0) {
            builder.config("spark.executor.cores", executorCores);
        }
        if (driverMemory != null && !driverMemory.isEmpty()) {
            builder.config("spark.driver.memory", driverMemory);
        }
        if (numExecutors > 0) {
            builder.config("spark.executor.instances", numExecutors);
        }

        // --- Cấu hình Jars ---
        if (jars != null && !jars.isEmpty()) {
            // Trong Java, sử dụng Collectors.joining() để nối các chuỗi bằng dấu phẩy
            // Lưu ý: "spark.jars" thường dùng cho file cục bộ, "spark.jars.packages"
            // dùng cho các tọa độ Maven. Ta dùng spark.jars để khớp với cách nối chuỗi của Python
            String jarsPath = jars.stream().collect(Collectors.joining(","));
            builder.config("spark.jar.packages", jarsPath);
        }

        // --- Cấu hình tùy chỉnh ---
        if (sparkConf != null) {
            sparkConf.forEach(builder::config);
        }

        SparkSession session = builder.getOrCreate();

        // Thiết lập mức độ log
        if (logLevel != null && !logLevel.isEmpty()) {
            session.sparkContext().setLogLevel(logLevel);
        }

        LOG.info("SparkSession '{}' đã được khởi tạo thành công.", appName);
        return session;
    }

    public SparkSession getSparkSession() {
        return this.spark;
    }

    public void stop() {
        if (this.spark != null) {
            try {
                this.spark.stop();
                LOG.info("SparkSession '{}' đã được dừng thành công.", appName);
            } catch (Exception e) {
                LOG.error("Lỗi khi dừng SparkSession: {}", e.getMessage(), e);
            } finally {
                this.spark = null;  // Giải phóng reference
            }
        } else {
            LOG.debug("SparkSession đã được dừng từ trước.");
        }
    }
}
