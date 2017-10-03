package services;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Client Node
 * @author Richie Yan
 * @since 30/09/2017 11:24 AM
 */
public class ClientService extends AbstractService implements Runnable {
    private Thread thread = new Thread(this);

    private static final Logger LOG = LoggerFactory.getLogger(ClientService.class);
    private ExecutorService executor = Executors.newSingleThreadExecutor();

    private List<String> onGoingTasks;

    private int producedCount = 0;


    @Override
    public void onStart() {
        onGoingTasks = new ArrayList<String>();
        thread.start();
    }

    @Override
    public void onStop() {
    }

    public void run() {

        register();

        while(producedCount < 10){
            try {
                Thread.sleep(RAND.nextInt(800) + 100);
                producedCount++;
                String name= queueCommand("CMD" + producedCount);
                System.out.println("Client "+ serverId +"  created " + name);
            } catch (InterruptedException e) {
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        this.stop();
    }

    //注册为工作节点
    void register() {
        zk.create("/workers/worker-" + serverId,
                new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, createWorkerCallback, null);
    }

    StringCallback createWorkerCallback = new StringCallback() {

        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
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
                    LOG.error("Something went wrong: " +
                            KeeperException.create(Code.get(rc), path));
            }
        }
    };


    Watcher newTaskWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if(e.getType() == EventType.NodeChildrenChanged) {
                assert ("/assign/worker-"+ serverId).equals( e.getPath() );
                getTasks();
            }
        }
    };


    void getTasks() {
        zk.getChildren("/assign/worker-" + serverId,
            newTaskWatcher, tasksGetChildrenCallback, null);
    }

    private DataCallback taskDataCallback = new DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {

        }
    };

    void submitTask(String task, TaskObject taskCtx) {
        taskCtx.setTask(task);
        zk.create("/tasks/task-", task.getBytes(),
                Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL, createTaskCallback, taskCtx);
    }

    private StringCallback createTaskCallback = new StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {

        }
    };

    ChildrenCallback tasksGetChildrenCallback = new ChildrenCallback() {

        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch(Code.get(rc)) {
                case CONNECTIONLOSS:
                    getTasks();
                    break;
                case OK:
                    if(children != null) {
                        //process task,we need run in another thread to avoid block callback
                        executor.execute(new Runnable() {
                            List<String> children;
                            DataCallback cb;
                            public Runnable init (List<String> children, DataCallback cb) {
                                this.children = children;
                                this.cb = cb;
                                return this;
                            }
                            @Override
                            public void run() {
                                LOG.info("Looping into tasks");
                                synchronized (onGoingTasks){
                                    for(String task:children){
                                        if(!onGoingTasks.contains(children)){
                                            LOG.trace("New task: {}", task);
                                            zk.getData("/assign/worker-" + serverId + "/" + task, false, cb, task);
                                            onGoingTasks.add( task );
                                        }
                                    }
                                }
                            }
                        }.init(children,taskDataCallback));
                    }
                    break;
                default:
                    LOG.error("getChildren failed: " +
                            KeeperException.create(Code.get(rc), path));
            }
        }
    };

    String queueCommand(String command) throws Exception {
        while(true){
            try {
                String name = zk.create("/tasks/task-",command.getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
                return name;
            } catch (KeeperException e) {
                if(e.code() == KeeperException.Code.NODEEXISTS){
                    throw new Exception(command + " already appears to be running");
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void process(WatchedEvent event) {
        switch (event.getType()){
            case NodeCreated:
                break;

        }
    }
}
