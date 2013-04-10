package org.monitoring.queryapi.preaggregation;

import java.util.concurrent.TimeUnit;
import org.monitoring.queryapi.Event;

/**
 *
 * @author Michal Dubravcik
 */
public interface Preaggregate {

    void saveEvent(TimeUnit unit, int[][] times, Event event);
    
}
