package services;

/**
 * @author Richie Yan
 * @since 30/09/2017 12:04 PM
 */
public interface ServiceEvent {
    int STARTED = 1;
    int STOPPED = 2;

    int getType();
}
