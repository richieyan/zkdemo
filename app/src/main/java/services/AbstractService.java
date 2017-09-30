package services;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base service for make a zk session
 * @author Richie Yan
 * @since 30/09/2017 12:09 PM
 */
public abstract class AbstractService implements Service,Watcher {

    //id for every service
    private final static AtomicInteger AUTO_ID = new AtomicInteger();
    private final static Logger LOG = LoggerFactory.getLogger(AbstractService.class);
    protected final static Random RAND = new Random();

    protected ZooKeeper zk;
    protected ServiceListener listener;
    protected String serverId;


    protected AbstractService(){
        serverId = String.valueOf(AUTO_ID.incrementAndGet());
    }
    public void start(ZookeeperCfg cfg,ServiceListener listener) {
        this.listener = listener;
        try {
            this.zk = new ZooKeeper(cfg.getConnectString(),15000,this);
        } catch (IOException e) {
            LOG.error("zk create failed",e);
        }
        this.onStart();
        this.listener.processEvent(new ServiceStartedEvent());
    }

    public void stop() {
        try {
            zk.close();
        } catch (InterruptedException e) {
            LOG.error("stop zk", e);
        }
        this.onStop();
        this.listener.processEvent(new ServiceStoppedEvent());
    }

    public abstract void onStart();
    public abstract void onStop();


}
