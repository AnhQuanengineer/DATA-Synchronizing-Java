package org.datapipeline.Config;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * Lớp trừu tượng cơ sở, chứa logic kiểm tra hợp lệ bằng Reflection.
 * (Mặc dù Builder đã kiểm tra, nhưng đây là một lớp phòng thủ bổ sung).
 */
public abstract class DatabaseConfig {

    /*
    Tôi vẫn giữ validate() này và gọi nó ở cuối constructor của các lớp cấu hình (new
    MongoDBConfig(this); bên trong build()) như một lớp phòng thủ cuối cùng. Tuy nhiên, việc
    kiểm tra chính đã được thực hiện bằng cách kiểm tra trực tiếp (if (field == null)) trong
    build(), giúp lỗi được phát hiện sớm hơn và code rõ ràng hơn. Đây chính là "Tách biệt Logic
    Validation" mà chúng ta đang nói tới.
     */
    public void validate() throws IllegalArgumentException {
        // Lấy tất cả các trường được khai báo trong lớp con
        for (Field field : this.getClass().getDeclaredFields()) {

            // Bỏ qua các hằng số tĩnh (ví dụ: collection="Users")
            if (Modifier.isStatic(field.getModifiers()) && Modifier.isFinal(field.getModifiers())) {
                continue;
            }

            field.setAccessible(true);
            try {
                Object value = field.get(this);

                // Cho phép các trường Optional như jarPath (nếu là null)
                if (field.getName().toLowerCase().contains("jarpath")) {
                    continue;
                }

                if (value == null) {
                    throw new IllegalArgumentException(
                            "----------Missing required config for " + field.getName() + " in " + this.getClass().getSimpleName() + "-------------"
                    );
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Error accessing field via reflection.", e);
            }
        }
    }
}