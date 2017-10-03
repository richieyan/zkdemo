package services;

import org.apache.zookeeper.*;
import org.apache.zookeeper.AsyncCallback.*;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
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
    private List<String> workerList = new ArrayList<String>();

    ChildrenCache workersCache;

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
        //工作节点
        createParent("/workers", new byte[0]);
        //分配节点
        createParent("/assign", new byte[0]);
        //任务
        createParent("/tasks", new byte[0]);
        //状态
        createParent("/status", new byte[0]);
    }

    //获得可以工作的节点，判断workers节点变化
    void getWorkers() {
        zk.getChildren("/workers",
            workersChangeWatcher, workersGetChildrenCallback, null);
    }

    //观察此节点，判断work节点的变化
    Watcher workersChangeWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if(e.getType() == Event.EventType.NodeChildrenChanged) {
                assert "/workers".equals( e.getPath() );
                getWorkers();
            }
        }
    };

    //workers节点
    ChildrenCallback workersGetChildrenCallback = new ChildrenCallback() {

        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    getWorkers();
                    break;
                case OK://当前的工作节点获取成功
                    LOG.info("Succesfully got a list of workers: "
                            + children.size()
                            + " workers");
                    reassignAndSet(children);
                    break;
                default:
                    LOG.error("getChildren failed",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    //设置工作节点缓存
    void reassignAndSet(List<String> workers) {
        List<String> toProcess;
        if(workersCache == null){//工作节点缓存为空，更新
            workersCache = new ChildrenCache(workers);
            toProcess = null;
        }else {//原先的工作节点缓存存在，更新工作节点
            LOG.info( "Removing and setting" );
            toProcess = workersCache.removedAndSet( workers );
        }

        if(toProcess != null) {//工作节点需要更新
            for(String worker : toProcess) {
                //If there is any worker that has been removed,
                // then we need to reassign its tasks.
                getAbsentWorkerTasks(worker);
            }
        }
    }

    void getAbsentWorkerTasks(String worker){
        zk.getChildren("/assign/" + worker, false, workerAssignmentCallback, null);
    }

    ChildrenCallback workerAssignmentCallback = new ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx, List<String> children){
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    getAbsentWorkerTasks(path);
                    break;
                case OK:
                    LOG.info("Succesfully got a list of assignments: "
                            + children.size()
                            + " tasks");
                    /*
                     * Reassign the tasks of the absent worker.
                     */
                    for(String task: children) {
                        getDataReassign(path + "/" + task, task);
                    }
                    break;
                default:
                    LOG.error("getChildren failed",  KeeperException.create(Code.get(rc), path));
            }
        }
    };

    /*
     ************************************************
     * Recovery of tasks assigned to absent worker. *
     ************************************************
     */

    /**
     * Get reassigned task data.
     *
     * @param path Path of assigned task
     * @param task Task name excluding the path prefix
     */
    void getDataReassign(String path, String task) {
        zk.getData(path,
                false,
                getDataReassignCallback,
                task);
    }

    /**
     * Get task data reassign callback.
     */
    DataCallback getDataReassignCallback = new DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)  {
            switch(Code.get(rc)) {
                case CONNECTIONLOSS:
                    getDataReassign(path, (String) ctx);

                    break;
                case OK:
//                    recreateTask(new RecreateTaskCtx(path, (String) ctx, data));

                    break;
                default:
                    LOG.error("Something went wrong when getting data ",
                            KeeperException.create(Code.get(rc)));
            }
        }
    };

    //获得任务列表
    void getTasks() {
        zk.getChildren("/tasks",
            tasksChangeWatcher, tasksGetChildrenCallback, null);
    }

    //任务列表发生变化，继续获取
    Watcher tasksChangeWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if(e.getType() == Event.EventType.NodeChildrenChanged) {
                assert "/tasks".equals( e.getPath() );
                getTasks();
            }
        }
    };
    //获得任务列表
    ChildrenCallback tasksGetChildrenCallback = new ChildrenCallback() {

        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch(Code.get(rc)) {
                case CONNECTIONLOSS:
                    getTasks();//重试
                    break;
                case OK:
                    if(children != null){//必须判断是否为null
                        assignTasks(children);//分配任务
                    }
                    break;
                default:
                    LOG.error("getChildren failed.", KeeperException.create(Code.get(rc), path));
            }
        }
    };

    void assignTasks(List<String> tasks) {
        for(String task : tasks) {
            getTaskData(task);
        }
    }

    //针对每个任务，获取其任务数据
    void getTaskData(String task) {
        zk.getData("/tasks/" + task,
            false, taskDataCallback, task);
    }

    //数据获取回调
    DataCallback taskDataCallback = new DataCallback() {

        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch(Code.get(rc)) {
                case CONNECTIONLOSS:
                    getTaskData((String) ctx);
                    break;
                case OK:
                    // Choose worker at random.
                    int worker = RAND.nextInt(workerList.size());
                    String designatedWorker = workerList.get(worker);
                    // Assign task to randomly chosen worker.
                    String assignmentPath = "/assign/" + designatedWorker + "/" + ctx;
                    createAssignment(assignmentPath, data);
                    break;
                default:
                    LOG.error("Error when trying to get task data.",
                            KeeperException.create(Code.get(rc), path));
                    break;
            }
        }
    };

    //创建一个任务分配节点/assign/worker/task(ID)
    void createAssignment(String path, byte[] data) {
        zk.create(path, data, Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT, assignTaskCallback, data);
    }

    //任务分配回调
    StringCallback assignTaskCallback = new StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) {
                case CONNECTIONLOSS:
                    createAssignment(path, (byte[]) ctx);
                    break;
                case OK:
                    //任务已经分配，删除任务
                    LOG.info("Task assigned correctly: " + name);
                    deleteTask(name.substring( name.lastIndexOf("/") + 1 ));
                    break;
                case NODEEXISTS:
                    //任务已经分配且已经被删除
                    LOG.warn("Task already assigned");
                    break;
                default:
                    LOG.error("Error when trying to assign task.",
                            KeeperException.create(Code.get(rc), path));
            } }
    };


    /*
     * Once assigned, we delete the task from /tasks
     */
    private void deleteTask(String name){
        zk.delete("/tasks/" + name, -1, taskDeleteCallback, null);
    }

    //任务删除回调
    private VoidCallback taskDeleteCallback = new VoidCallback(){
        public void processResult(int rc, String path, Object ctx){
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    deleteTask(path);
                    break;
                case OK:
                    LOG.info("Successfully deleted " + path);
                    break;
                case NONODE:
                    LOG.info("Task has been deleted already");
                    break;
                default:
                    LOG.error("Something went wrong here, " +
                            KeeperException.create(Code.get(rc), path));
            }
        }
    };

    //异步方式创建master
    private void runForMaster(){
        zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL, masterCreateCallback, this);
    }

    private StringCallback masterCreateCallback = new StringCallback() {
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
    private DataCallback masterCheckCallback = new DataCallback() {
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

        //获得工作节点
        LOG.info("Going for list of workers");
        getWorkers();

//        (new RecoveredAssignments(zk)).recover( new RecoveryCallback() {
//            public void recoveryComplete(int rc, List<String> tasks) {
//                if(rc == RecoveryCallback.FAILED) {
//                    LOG.error("Recovery of assigned tasks failed.");
//                } else {
//                    LOG.info( "Assigning recovered tasks" );
//                    getTasks();
//                }
//            }
//        });

    }

    //主要目的是添加Watcher：节点是否存在，并添加Watcher
    //与checkMaster区别是，checkMaster无Watcher
    private void masterExists() {
        LOG.info("serverId = {} is not leader",serverId);
        zk.exists("/master", masterExistsWatcher, masterExistsCallback, null);
    }

    private StatCallback masterExistsCallback = new StatCallback() {
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

    private StringCallback createParentCallback = new StringCallback() {
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
