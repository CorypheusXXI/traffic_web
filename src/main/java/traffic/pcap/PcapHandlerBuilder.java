package traffic.pcap;

import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.pcap4j.core.PcapHandle;
import org.pcap4j.core.PcapNetworkInterface;
import org.pcap4j.core.Pcaps;
import traffic.config.Config;

import java.net.InetAddress;

/**
 * Утилитный класс для создания {@link PcapHandle}
 * Параметры, отличные от параметов по-умолчанию, можно задавать системными переменными, например:
 * java -Dproperty=10
 *
 * или
 *
 * System.setProperty("property","propertyValue");
 *
 * @author Roman Rumiantsev
 */
@Slf4j
@UtilityClass
public class PcapHandlerBuilder {

    @SneakyThrows
    public PcapHandle buildHandler() {
        log.debug("buildHandler.enter;");
        InetAddress addr = InetAddress.getByName(Config.LISTEN_ADDRESS);
        PcapNetworkInterface nif = Pcaps.getDevByAddress(addr);
        PcapHandle.Builder phb
                = new PcapHandle.Builder(nif.getName())
                .snaplen(Config.Pcap.SNAPLEN)
                .promiscuousMode(PcapNetworkInterface.PromiscuousMode.PROMISCUOUS)
                .timeoutMillis(Config.Pcap.READ_TIMEOUT)
                .bufferSize(Config.Pcap.BUFFER_SIZE);
        if (Config.Pcap.TIMESTAMP_PRECISION_NANO) {
            phb.timestampPrecision(PcapHandle.TimestampPrecision.NANO);
        }
        PcapHandle handler = phb.build();
        log.debug("buildHandler.exit;");
        return handler;
    }
}
