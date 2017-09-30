package services;

/**
 * @author Richie Yan
 * @since 30/09/2017 12:06 PM
 */
public class ServiceStoppedEvent implements ServiceEvent {
    public int getType() {
        return STOPPED;
    }
}
