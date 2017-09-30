package services;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Master Node
 * @author Richie Yan
 * @since 30/09/2017 11:24 AM
 */
public class MasterService extends AbstractService implements Runnable {
    private Thread thread = new Thread(this);
    private int consumedCount = 0;
    private boolean isLeader;
    private static Logger LOG = LoggerFactory.getLogger(MasterService.class);

    private static AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            MasterService service = (MasterService)ctx;
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    service.checkMaster();
                    return;
                case OK:
                    service.isLeader = true;
                    break;
                default:
                    service.isLeader = false;
            }
            LOG.info("id:{} is {} the leader", service.serverId, (service.isLeader ? "" : "not"));

            if(service.isLeader){
                service.bootstrap();
            }
        }
    };

    private static AsyncCallback.DataCallback masterCheckCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            MasterService service = (MasterService)ctx;
            switch(KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    service.checkMaster();
                    break;
                case NONODE:
                    service.runForMaster();
                    break;
            }
        }
    };


    @Override
    public void onStart() {
        thread.start();
    }

    @Override
    public void onStop() {

    }

    public void run() {
        runForMaster();
        while(consumedCount < 10){
            try {
                Thread.sleep(RAND.nextInt(1000) + 500);
                consumedCount++;
            } catch (InterruptedException e) {
            }
        }
        this.stop();
    }


    private AsyncCallback.StringCallback createParentCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    createParent(path,(byte[])ctx);
                    break;
                case OK:
                    LOG.info("Parent created");
                    break;
                case NODEEXISTS:
                    LOG.warn("Parent already registered: {}", path);
                    break;
                default:
                    LOG.error("Something went wrong: ",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    void createParent(String path, byte[] data){
        zk.create(path,data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,createParentCallback,data);
    }
    //创建协助节点，持久类型
    private void bootstrap() {
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    //异步方式
    private void runForMaster(){
        zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL, masterCreateCallback, this);
    }
    private void checkMaster() {
        zk.getData("/master", false, masterCheckCallback, this);
    }
    public void process(WatchedEvent event) {

    }
}
