package services;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Richie Yan
 * @since 30/09/2017 4:37 PM
 */
public class WorkService extends AbstractService implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(WorkService.class);

    private int workCount = 0;

    private String status;

    @Override
    public void onStart() {

    }

    @Override
    public void onStop() {

    }

    @Override
    public void run() {
        register();

        while(workCount < 10){
            try {
                Thread.sleep(RAND.nextInt(1000) + 500);
                workCount++;
            } catch (InterruptedException e) {
            }
        }
    }

    @Override
    public void process(WatchedEvent e) {
        LOG.info(e.toString() + ", ");
    }

    private AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    register();
                    break;
                case OK:
                    LOG.info("Registered successfully: " + serverId);
                    break;
                case NODEEXISTS:
                    LOG.warn("Already registered: " + serverId);
                    break;
                default:
                    LOG.error("Something went wrong: "
                            + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };


    private AsyncCallback.StatCallback statusUpdateCallback = new AsyncCallback.StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch(KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                updateStatus((String)ctx);
                return;
            }
        }
    };

    private void updateStatus(String ctx) {
        if(ctx == this.status){//if the status is not the status we set by ref
            zk.setData("/workers/worker-" + serverId, status.getBytes(), -1,
                    statusUpdateCallback, status);
        }
    }

    public void setStatus(String status){
        this.status = status;
        updateStatus(status);
    }

    void register() {
        zk.create("/workers/worker-" + serverId,
            "Idle".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL, createWorkerCallback, null);
    }
}
