package org.datapipeline.Models;

import java.util.Map;

public class MessageRecord {
    private final long count;
    private final Map<String, Object> data;

    public MessageRecord(long count, Map<String, Object> data) {
        this.count = count;
        this.data = data;
    }

    public long getCount() {
        return count;
    }

    public Map<String, Object> getData() {
        return data;
    }

    /*
    *Mối liên hệ với equals(): Vì equals() chỉ dựa vào data để so sánh, hashCode() cũng
    *chỉ dựa vào data để tính toán mã băm, tuân thủ nguyên tắc ràng buộc của Java. Hai đối
    *tượng có cùng data (bằng nhau) sẽ trả về cùng một mã băm, đảm bảo chúng hoạt động
    *chính xác trong HashMap hoặc HashSet
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true; //1. Tối ưu: Nếu cùng là một đối tượng trong bộ nhớ, trả về true ngay
        if (!(o instanceof MessageRecord)) return false; //2. Kiểm tra kiểu: Nếu o không phải là MessageRecord, trả về false
        MessageRecord that = (MessageRecord) o; // 3. Ép kiểu: Chắc chắn o là MessageRecord, ép kiểu để truy cập các trường
        return data.equals(that.data);// 4. So sánh giá trị: Hai đối tượng bằng nhau nếu trường 'data' của chúng bằng nhau.
    }

    @Override
    public int hashCode() {
        return data.hashCode();
    }

    @Override
    public String toString() {
        return "MessageRecord{count=" + count + ", data=" + data + "}";
    }
}

