package topacademy;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import topacademy.config.KafkaConfig;
import topacademy.domain.Symbol;

import java.awt.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

/**
 * Webinar-02: Kafka producer-service (отправка объектов класса Person)
 * Использования метода producer.send(producerRecord) с обработкой результата отправки через Callback.
 */
public class KafkaProducerApp {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerApp.class);
    private static final String string = "fghfghfghaadgfnbvertgxjhnaldk,dfsf,;f";

    public static void main(String[] args) {
        Pattern VOWELS_PATTERN = Pattern.compile("[aeiou]", Pattern.CASE_INSENSITIVE);

        try (KafkaProducer<Long, Symbol> producer = new KafkaProducer<>(KafkaConfig.getProducerConfig())) {

            System.out.println("Start...");

            for (int i = 0; i < string.length(); i++) {
                String s = string.substring(i, i + 1);
                Symbol symbol = createSymbol(i, s);
                /**
                 * Конструктор ProducerRecord(topic, partition, timestamp, key, value) принимает в качестве аргументов:
                 * - topic - номер топика
                 * - partition - номер партиции           (опция)
                 * - timestamp - время создания сообщения (опция)
                 * - key - ключ id экземпляра Symbol      (опция)
                 * - value - объект Symbol
                 *
                 * Варианты конструкторов:
                 * - ProducerRecord(topic, value)
                 * - ProducerRecord(topic, key, value)
                 * - ProducerRecord(topic, partition, key, value)
                 * - ProducerRecord(topic, partition, key, value, headers)
                 */
                long timestamp = System.currentTimeMillis();

                Boolean isVowels = VOWELS_PATTERN.matcher(s).matches();

                ProducerRecord<Long, Symbol> producerRecord = new ProducerRecord<>(
                        isVowels ? KafkaConfig.TOPIC1 : KafkaConfig.TOPIC2,
                        KafkaConfig.PARTITION,
                        timestamp, symbol.getId(), symbol);

                /**
                 * Анонимный внутренний класс (Callback), содержащий только один метод onCompletion(), можно записать
                 * через лямбду
                 */
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Error sending message: {}", e.getMessage(), e);
                        } else {
                            logger.info("Sent record: key={}, value={}, partition={}, offset={}",
                                    symbol.getId(), symbol, recordMetadata.partition(), recordMetadata.offset());                        }
                    }
                });
                logger.info("Отправлено сообщение: key-{}, value-{}", i, symbol);
            }
            logger.info("Отправка завершена.");
        } catch (Exception e) {
            logger.error("Ошибка при отправке сообщений в Kafka", e);
        }
    } // todo показать без try -with-resources c вызовом .flush() .close() .close(Duration.ofSeconds(60))

    private static Symbol createSymbol(int index, String ch) {
        // String currentTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("dd-MM-yyyy-HH-mm-ss"));
        Pattern VOWELS_PATTERN = Pattern.compile("[aeiou]", Pattern.CASE_INSENSITIVE);
        Boolean isVowels = VOWELS_PATTERN.matcher(ch).matches();

        return new Symbol(index, ch, isVowels ? "RED" : "GREEN", isVowels ? 1 : 2);
    }

}
