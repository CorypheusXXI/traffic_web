package traffic.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import traffic.config.Config;

import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Класс отвечающий за обработку сообщений Kafka
 *
 * @author Roman Rumiantsev
 */
@Slf4j
public class AlertConsumer {

    private AlertConsumer() {
    }

    /**
     * KafkaConsumer подписывается на "alert" топик брокера и отображает сообщения
     * Метод выполняется в отдельном потоке
     */
    public static void startReceivingMessages() {
        log.debug("startReceivingMessages.enter;");

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> {
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Config.Kafka.getProperties())) {

                consumer.subscribe(Collections.singletonList(Config.Kafka.ALERT_TOPIC));

                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {
                        String value = record.value();
                        if (Objects.equals(value, "true")) {
                            log.info("Alert! For the last 10 minutes the traffic limits were exceeded!");
                        } else {
                            log.info("Alert! The traffic limits normalized!");
                        }
                    }
                }
            } catch (Exception e) {
                log.error("Error while receiving alert messages, due to ", e.getMessage(), e);
            }
        });
        executorService.shutdown();
        log.debug("startReceivingMessages.exit;");
    }
}
