package topacademy.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import topacademy.domain.Symbol;

import java.util.Map;

/* SymbolSerializer используется для преобразования объектов Symbol в байтовый массив при отправке в Kafka.*/
public class SymbolSerializer implements Serializer<Symbol> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Symbol data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException("Error serializing Person to JSON", e);
        }
    }

    @Override
    public void close() {
    }
}
