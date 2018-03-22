package traffic;

import lombok.extern.slf4j.Slf4j;
import traffic.handler.TrafficCatcherProcessor;
import traffic.kafka.AlertConsumer;

/**
 * Main-class приложения по отслеживанию трафика
 *
 * @author Roman Rumiantsev
 */
@Slf4j
public class Application {

    public static void main(String[] args) {
        log.info("Traffic Catcher Application started!");

        log.debug("Starting Kafka Consumer...");
        AlertConsumer.startReceivingMessages();

        log.debug("Initializing TrafficCatcherProcessor...");
        TrafficCatcherProcessor processor = new TrafficCatcherProcessor();
        processor.catchTraffic();

        log.info("Traffic Catcher Application finished!");
    }

}
