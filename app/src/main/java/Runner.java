import services.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 基本想法是构建一个生产者消费者的服务模型，可以自由创建多个生产者和消费的服务。
 * 多个服务之间通过zookeeper进行协调处理生产者和消费者的任务管理。
 * 具体到现实中，一般可能存在大量的生产者，前端用户产生的行为，而消费者是固定的后端服务器。
 * 假设使用zookeeper的主从结构
 * 生产者：创建任务到zookeeper
 * 消费者：master节点检测并分配任务，从节点处理任务。
 * @author Richie Yan
 * @since 30/09/2017 11:29 AM
 */
public class Runner implements ServiceListener {
    private CountDownLatch latch;
    private int serviceCount = 0;
    private List<Service> services;
    private ZookeeperCfg cfg;

    public Runner(ZookeeperCfg cfg){
        this.services = new ArrayList<Service>();
        this.cfg = cfg;
    }

    public void startService(Service service){
        services.add(service);
        service.start(cfg,this);
    }

    public void waitServiceComplete() throws InterruptedException {
        latch = new CountDownLatch(serviceCount);
        latch.await();
    }

    public static void main(String[] args) throws InterruptedException {
        ZookeeperCfg cfg = new ZookeeperCfg();
        cfg.setConnectString("127.0.1:2181");
        Runner runner = new Runner(cfg);

        runner.startService(new AdminClient());
        runner.startService(new MasterService());
        runner.startService(new MasterService());
        runner.startService(new MasterService());

        runner.waitServiceComplete();

        Thread.sleep(10000);
    }

    public void processEvent(ServiceEvent e) {
        switch (e.getType()) {
            case ServiceEvent.STARTED:
                serviceCount ++;
                break;
            case ServiceEvent.STOPPED:
                latch.countDown();
                break;
        }
    }
}
