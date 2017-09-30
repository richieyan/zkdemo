package services;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;

/**
 * Client Node
 * @author Richie Yan
 * @since 30/09/2017 11:24 AM
 */
public class ClientService extends AbstractService implements Runnable {
    private Thread thread = new Thread(this);

    private int producedCount = 0;

    @Override
    public void onStart() {
        thread.start();
    }

    @Override
    public void onStop() {
    }

    public void run() {
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

    String queueCommand(String command) throws Exception {
        while(true){
            try {
                String name = zk.create("/tasks/task-",command.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
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
