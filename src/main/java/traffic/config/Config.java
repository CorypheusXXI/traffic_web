package traffic.config;

import lombok.experimental.UtilityClass;
import org.apache.spark.SparkConf;

import java.util.Optional;
import java.util.Properties;

/**
 * Конфигурации проекта
 *
 * @author Roman Rumiantsev
 */
public class Config {

    public static final String LISTEN_ADDRESS = Optional.ofNullable(System.getProperty("listen.address"))
            .orElse("192.168.0.13");

    private Config() {}

    @UtilityClass
    public static class Spark {
        public final String APPLICATION_NAME = "Traffic Application";
        public final String LOCAL_MASTER = "local[*]";
        public final Long SECOND = 1000L;
        public final Long CAPTURE_WINDOW_DURATION = 300 * SECOND;

        private SparkConf conf = new SparkConf();

        public SparkConf getSparkConfiguration() {
            conf.setAppName(APPLICATION_NAME);
            return conf;
        }
    }

    @UtilityClass
    public static class Pcap {

        public final int READ_TIMEOUT
                = Integer.getInteger("pcap.readTimeout", 10);
        public final int SNAPLEN
                = Integer.getInteger("pcap.snaplen", 65536);
        public final boolean TIMESTAMP_PRECISION_NANO
                = Boolean.getBoolean("pcap.timestampPrecision.nano");
        public final int BUFFER_SIZE
                = Integer.getInteger("pcap.bufferSize", 1024 * 1024);
    }

    @UtilityClass
    public static class Kafka {

        public final String ALERT_TOPIC = "alert";
        private Properties properties = new Properties();

        public Properties getProperties() {

            properties.put("bootstrap.servers", LISTEN_ADDRESS + ":9092");
            properties.put("key.deserializer",
                           "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("value.deserializer",
                           "org.apache.kafka.common.serialization.StringDeserializer");
            properties.put("key.serializer",
                           "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer",
                           "org.apache.kafka.common.serialization.StringSerializer");
            return properties;
        }
    }
}
