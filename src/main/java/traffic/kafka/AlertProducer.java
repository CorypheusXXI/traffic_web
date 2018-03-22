package traffic.kafka;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import traffic.config.Config;

/**
 * Класс отвечающий за генерацию сообщений в Kafka
 *
 * @author Roman Rumiantsev
 */
@NoArgsConstructor
@Slf4j
public class AlertProducer {

    @Getter
    @Setter
    private boolean currentlyFailed = false;

    /**
     * Метод по отправке KafkaProducer'ом сообщений с топиком "alert"
     */
    public void sendMessage() {
        log.debug("sendMessage.enter;");

        try (Producer<String, String> producer = new KafkaProducer<>(Config.Kafka.getProperties())) {
            currentlyFailed ^= true;
            producer.send(new ProducerRecord<>(Config.Kafka.ALERT_TOPIC,
                                               String.valueOf(currentlyFailed)));
        }
        log.debug("sendMessage.exit;");
    }
}
