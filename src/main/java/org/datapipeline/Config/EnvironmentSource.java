package org.datapipeline.Config;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Triển khai ConfigurationSource, đọc cấu hình từ file .env (nếu có) và biến môi trường hệ thống.
 */
public class EnvironmentSource implements ConfigurationSource {

    private final Properties properties;
    private final InputStream inputStream;

    public EnvironmentSource() throws IOException{
        this.inputStream = new FileInputStream("config.properties");
        this.properties = new Properties();
        this.properties.load(this.inputStream); // Load một lần
        this.inputStream.close(); // Đóng ngay sau khi load
    }

    @Override
    public String getString(String key) throws IOException {
        if (properties.containsKey(key)) {
            return properties.getProperty(key);
        }
        return null;
    }

    public void close() throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
    }

//    public static void main(String[] args) {
//        EnvironmentSource reader = null;
//        try {
//            reader = new EnvironmentSource();
//            String haha = reader.getString("MONGO_URI");
//            System.out.println(haha);
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            if (reader != null) {
//                try {
//                    reader.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//    }
}