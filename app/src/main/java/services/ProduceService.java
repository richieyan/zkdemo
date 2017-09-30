package services;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;

/**
 * @author Richie Yan
 * @since 30/09/2017 11:24 AM
 */
public class ProduceService extends AbstractService implements Runnable {
    private Thread thread = new Thread(this);

    private int producedCount = 0;

    @Override
    public void onStart() {
        thread.start();
    }

    @Override
    public void onStop() {
    }

    public void run() {
        while(producedCount < 10){
            try {
                Thread.sleep(500);
                producedCount++;
                System.out.println("produced count--->" + producedCount);
            } catch (InterruptedException e) {
            }
        }
        this.stop();
    }

    public void process(WatchedEvent event) {
        System.out.println(event);
    }
}
