package services;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

/**
 * Admin Client
 * list information
 * @author Richie Yan
 * @since 30/09/2017 4:56 PM
 */
public class AdminClient extends AbstractService implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(AdminClient.class);
    private Thread thread = new Thread(this);
    private boolean running = false;
    private AsyncCallback.StatCallback onMasterChange = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            LOG.info("path--->{},{}",path,KeeperException.Code.get(rc));
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                case NONODE:
                    if(!running){
                        checkMaster();
                    }
                    break;
                case OK:
                    LOG.info("start running--->true");
                    running = true;
                    thread.start();
                    break;
        }
    }
    };

    @Override
    public void onStart() {
        checkMaster();
    }

    private void checkMaster(){
        LOG.info("check master---->");
        //检测master节点，并设置监听此path的后续变化
        zk.exists("/master",true,onMasterChange,null);
    }

    @Override
    public void onStop() {

    }

    @Override
    public void process(WatchedEvent event) {
        LOG.info("process {} ", event);

        // 当path不为null并且为master时，
        // 判断是否为节点删除，如果是删除，停止AdminClient
        if(event.getPath()!=null && event.getPath().equals("/master")){
            if(event.getType() == Event.EventType.NodeDeleted){
                running = false;
            }
        }
    }

    void listState() throws KeeperException, InterruptedException {
        try {
            Stat stat = new Stat();
            byte masterData[] = zk.getData("/master", false, stat);
            Date startDate = new Date(stat.getCtime());
            LOG.info("Master: serverId=" + new String(masterData) +  " since " + startDate);
        } catch (KeeperException.NoNodeException e) {
            LOG.info("No Master");
        }

        StringBuilder sb = new StringBuilder("Workers:");
        for (String w: zk.getChildren("/workers", false)) {
            byte data[] = zk.getData("/workers/" + w, false, null);
            String state = new String(data);
            sb.append("\t" + w + ": " + state);
        }
        LOG.info(sb.toString());

        sb = new StringBuilder("Tasks:");
        List<String> children = zk.getChildren("/assign", false);
        for (String t: children) {
            sb.append('\t').append(t);
        }
        LOG.info(sb.toString());
    }

    @Override
    public void run() {
        while (running){
            try {
                listState();
                Thread.sleep(3000);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        this.stop();

        LOG.info("stopped");
    }
}
