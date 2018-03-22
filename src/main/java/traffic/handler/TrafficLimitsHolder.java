package traffic.handler;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import java.util.Objects;
import java.util.Optional;

/**
 * Класс отвечающий за извлечение limits из Hive
 *
 * @author Roman Rumiantsev
 */
@Slf4j
public class TrafficLimitsHolder {

    private static final String SQL = "select * from traffic_limits.limits_per_hour";
    @Getter
    private Long minLimit;
    @Getter
    private Long maxLimit;

    public TrafficLimitsHolder() {
        this.minLimit = 0L;
        this.maxLimit = 0L;
    }

    /**
     * Метод извлекающий пределы максимального и минимального объема траффика из Hive
     * Значения записываются в {@link #maxLimit} и {@link #minLimit} соответственно.
     */
    public void extractData() {
        log.debug("extractData.enter;");
        SparkContext sc = new SparkContext();

        HiveContext hiveContext = new HiveContext(sc);
        DataFrame rows = hiveContext.sql(SQL);
        rows.collectAsList().forEach(row -> {
            String key = Optional.ofNullable(row.get(0))
                    .map(Object::toString)
                    .orElse("");
            Long value = Optional.ofNullable(row.get(1))
                    .map(Object::toString)
                    .map(Long::valueOf)
                    .orElse(0L);
            if (Objects.equals("min", key)) {
                minLimit = value;
            }
            if (Objects.equals("max", key)) {
                maxLimit = value;
            }
        });

        sc.stop();
        log.debug("extractData.exit;");
    }
}
