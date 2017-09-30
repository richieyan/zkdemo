package services;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * @author Richie Yan
 * @since 30/09/2017 12:09 PM
 */
public abstract class AbstractService implements Service,Watcher {
    protected ServiceListener listener;

    protected ZooKeeper zk;

    public void start(ZookeeperCfg cfg,ServiceListener listener) {
        this.listener = listener;
        try {
            this.zk = new ZooKeeper(cfg.getConnectString(),15000,this);
        } catch (IOException e) {
            System.out.println(e);
        }
        this.onStart();
        this.listener.processEvent(new ServiceStartedEvent());
    }

    public void stop() {
        this.onStop();
        this.listener.processEvent(new ServiceStoppedEvent());
    }

    public abstract void onStart();
    public abstract void onStop();
}
