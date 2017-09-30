package services;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Richie Yan
 * @since 30/09/2017 3:43 PM
 */
public class SyncMasterService extends AbstractService implements Watcher,Runnable{

    private boolean isLeader;
    private int consumedCount = 0;

    @Override
    public void onStart() {

    }

    @Override
    public void onStop() {

    }

    @Override
    public void process(WatchedEvent event) {

    }

    boolean checkMasterSync() {
        while (true) {
            try {
                Stat stat = new Stat();
                byte data[] = zk.getData("/master", false, stat);
                isLeader = new String(data).equals(serverId);
                return true;
            } catch (KeeperException.NoNodeException e) {
                // no master, so try create again
                System.out.println("try again---->");
                return false;
            } catch (KeeperException.ConnectionLossException e) {
            } catch (InterruptedException e) {
            } catch (KeeperException e) {
                e.printStackTrace();
            }
        }
    }

    //同步方式创建
    void runForMaster() throws InterruptedException {
        while (true){
            try{
                zk.create("/master", serverId.getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                isLeader = true;
                System.out.println("isLeader: serverId=" + serverId);
                break;
            } catch (KeeperException e) {
                KeeperException.Code code = e.code();
                if(code == KeeperException.Code.NODEEXISTS){
                    isLeader = false;
                    System.out.println("isNotLeader: serverId=" + serverId);
                    break;
                }else if(code == KeeperException.Code.CONNECTIONLOSS){

                }
            }

            if(checkMasterSync()) {
                break;
            }
        }
    }

    @Override
    public void run() {
        try {
            runForMaster();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        while(consumedCount < 10){
            try {
                Thread.sleep(RAND.nextInt(1000) + 500);
                consumedCount++;
            } catch (InterruptedException e) {
            }
        }
        this.stop();
    }
}
