package traffic.handler;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import traffic.config.Config;
import traffic.kafka.AlertProducer;
import traffic.pcap.PcapCustomReceiver;

/**
 * Класс отвечающий за отслеживания трафика
 *
 * @author Roman Rumiantsev
 */
@Slf4j
public class TrafficCatcherProcessor {

    private TrafficLimitsHolder trafficLimitsHolder;
    private AlertProducer alertProducer;

    public TrafficCatcherProcessor() {
        alertProducer = new AlertProducer();
        trafficLimitsHolder = new TrafficLimitsHolder();
        trafficLimitsHolder.extractData();
    }


    /**
     * Основной метод по отслеживанию трафика
     */
    public void catchTraffic() {
        log.debug("TrafficCatcherProcessor.enter;");
        SparkConf conf = Config.Spark.getSparkConfiguration();
//        conf.setMaster(Config.Spark.LOCAL_MASTER);

        try (JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(Config.Spark.SECOND))) {
            JavaReceiverInputDStream<String> lines = ssc.receiverStream(
                    new PcapCustomReceiver());
            JavaDStream<Long> result = lines.map(Long::parseLong)
                    .reduceByWindow((Function2<Long, Long, Long>) (v1, v2) -> v1 + v2,
                                    new Duration(Config.Spark.CAPTURE_WINDOW_DURATION),
                                    new Duration(Config.Spark.SECOND));
            result.foreachRDD(rdd -> {
                Long deltaTraffic = rdd.take(1).stream()
                        .findFirst()
                        .orElse(0L);
                if (isCrossLimits(deltaTraffic)) {
                    alertProducer.sendMessage();
                }
            });
            ssc.start();
            ssc.awaitTermination();
        } catch (Exception e) {
            log.error("Error catching traffic, cause of {}", e.getMessage(), e);
        }
        log.debug("TrafficCatcherProcessor.exit;");
    }

    /**
     * Метод проверяет выход объема траффика за пределы минимального и максимального значения
     *
     * Так же меняет значение флага, если траффик перешел границы допустимых значений в обе стороны
     * по сравнению с предыдущими итерациями
     *
     * Если траффик был в допустимых пределах, но на текущем шаге вышел за них - ставится true
     * Если траффик вышел за пределы на предыдущих итерациях, но вернулся обратно - ставится false
     *
     * @param traffic - объем трафика
     * @return - перешел ли текущий трафик границы по сравнению с предыдущими итерациями
     */
    private boolean isCrossLimits(Long traffic) {
        boolean outOfLimits = traffic.compareTo(trafficLimitsHolder.getMinLimit()) < 0 ||
                traffic.compareTo(trafficLimitsHolder.getMaxLimit()) > 0;
        boolean crossLimits = false;
        if (!alertProducer.isCurrentlyFailed() && outOfLimits) {
            alertProducer.setCurrentlyFailed(true);
            crossLimits = true;
        }
        if (alertProducer.isCurrentlyFailed() && !outOfLimits) {
            alertProducer.setCurrentlyFailed(false);
            crossLimits = true;
        }
        return crossLimits;
    }
}
