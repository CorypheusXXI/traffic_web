package traffic.pcap;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.pcap4j.core.NotOpenException;
import org.pcap4j.core.PcapHandle;
import org.pcap4j.packet.Packet;

/**
 * Кастомный SparkStreamReceiver для отслеживания Pcap трафика в потоке
 *
 * @author Roman Rumiantsev
 */
@Slf4j
public class PcapCustomReceiver extends Receiver<String> {

    public PcapCustomReceiver() {
        super(StorageLevel.MEMORY_AND_DISK_2());
    }

    @Override
    public void onStart() {
        new Thread(this::receive).start();
    }

    @Override
    public void onStop() {
        //nothing
    }

    private void receive() {
        log.debug("receiver.enter;");
        PcapHandle handler = PcapHandlerBuilder.buildHandler();
        while(!isStopped()){
            Packet nextPacket = null;
            try {
                nextPacket = handler.getNextPacket();
            } catch (NotOpenException e) {
                log.error("Could not open packet, {}", e.getMessage(), e);
            }
            if (nextPacket != null) {
                store(Integer.toString(nextPacket.length()));
            }
        }
        log.debug("receiver.exit;");
    }
}
