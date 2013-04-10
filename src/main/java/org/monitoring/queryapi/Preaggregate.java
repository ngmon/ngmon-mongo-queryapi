package org.monitoring.queryapi;

import java.util.concurrent.TimeUnit;

/**
 *
 * @author Michal Dubravcik
 */
public interface Preaggregate {

    void saveEvent(TimeUnit unit, int[][] times, Event event);
    
}
