package services;

/**
 * @author Richie Yan
 * @since 30/09/2017 11:46 AM
 */
public interface Service {
    void start(ZookeeperCfg cfg, ServiceListener listener);
    void stop();
}
