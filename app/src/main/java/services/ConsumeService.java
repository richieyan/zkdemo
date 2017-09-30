package services;

import org.apache.zookeeper.WatchedEvent;

/**
 * @author Richie Yan
 * @since 30/09/2017 11:24 AM
 */
public class ConsumeService extends AbstractService implements Runnable {
    private Thread thread = new Thread(this);

    private int consumedCount = 0;
    @Override
    public void onStart() {
        thread.start();
    }

    @Override
    public void onStop() {

    }

    public void run() {
        while(consumedCount < 10){
            try {
                Thread.sleep(500);
                consumedCount++;
            } catch (InterruptedException e) {

            }
        }
        this.stop();
    }

    public void process(WatchedEvent event) {

    }
}
