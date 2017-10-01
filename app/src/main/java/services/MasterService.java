package services;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Master Node
 * @author Richie Yan
 * @since 30/09/2017 11:24 AM
 */
public class MasterService extends AbstractService implements Runnable {

    private static Logger LOG = LoggerFactory.getLogger(MasterService.class);

    private Thread thread = new Thread(this);
    private static AtomicBoolean INIT_DONE = new AtomicBoolean(false);
    private boolean isLeader;


    private enum MasterStates{
        NOT_ELECTED, RUNNING, ELECTED

    }

    private MasterStates state;

    private long deadline;

    public MasterService(int seconds){
        deadline = System.currentTimeMillis() + (long)seconds * 1000L;
    }


    @Override
    public void onStart() {
        LOG.info("Start service--->{}",serverId);
        runForMaster(); //尝试创建
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {

        }
        thread.start();
    }

    @Override
    public void onStop() {

    }

    public void run() {
        while(deadline > System.currentTimeMillis()){
            try {
                Thread.sleep(RAND.nextInt(1000) + 500);
            } catch (InterruptedException e) {
            }
        }
        this.stop();
    }

    //创建协助节点，持久类型
    private void bootstrap() {
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    //异步方式创建master
    private void runForMaster(){
        zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL, masterCreateCallback, this);
    }

    private AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();//重试，先检测是否存在，然后继续runForMaster
                    break;
                case OK: //创建成功
                    state = MasterStates.ELECTED;
                    takeLeadership();//设置自己为主节点
                    break;
                case NODEEXISTS: //节点已经存在
                    state = MasterStates.NOT_ELECTED;
                    masterExists();//判断节点是否存在，同时增加一个Watcher
                    break;
                default: //其他错误类型
                    state = MasterStates.NOT_ELECTED;
                    LOG.error("Something went wrong when running for master.",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }

        }
    };


    //尝试获得master的数据
    private void checkMaster() {
        zk.getData("/master", false, masterCheckCallback, this);
    }

    //检测回调
    private AsyncCallback.DataCallback masterCheckCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch(KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster(); //重试
                    break;
                case NONODE:
                    runForMaster(); //再次创建
                    break;
            }
        }
    };

    //设置自己为Leader
    private void takeLeadership(){
        isLeader = true;
        LOG.info("takeLeadership#serverId={} is the Leader",serverId);
        if(!INIT_DONE.get()) {
            bootstrap();
            INIT_DONE.set(true);
        }

    }

    //主要目的是添加Watcher：节点是否存在，并添加Watcher
    //与checkMaster区别是，checkMaster无Watcher
    private void masterExists() {
        LOG.info("serverId = {} is not leader",serverId);
        zk.exists("/master", masterExistsWatcher, masterExistsCallback, null);
    }

    private AsyncCallback.StatCallback masterExistsCallback = new AsyncCallback.StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    masterExists();//基本上这个错误，都是重试
                    break;
                case OK:
                    if(stat == null) { //响应OK，但是节点不存在(stat为null表示节点不存在)
                        state = MasterStates.RUNNING;
                        runForMaster();
                    }
                    break;
                default:
                    // 尝试获得/master下数据，判断是否需要重新设置或创建节点
                    checkMaster();//从源头开始重试
                    break;
            }
        }
    };

    private Watcher masterExistsWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if(e.getType() == Event.EventType.NodeDeleted) {
                assert "/master".equals( e.getPath() );
                runForMaster();//有节点删除，尝试创建master
            }
    } };



    //create parent node for tasks
    void createParent(String path, byte[] data){
        zk.create(path,data,ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,createParentCallback,data);
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

    public void process(WatchedEvent event) {
    }
}
