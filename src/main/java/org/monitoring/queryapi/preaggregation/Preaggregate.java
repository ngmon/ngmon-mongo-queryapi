package org.monitoring.queryapi.preaggregation;

import java.util.concurrent.TimeUnit;
import org.monitoring.queryapi.Event;

/**
 * Precounting of statistics
 * @author Michal Dubravcik
 */
public interface Preaggregate {

    /**
     * Save event and actualize statistics
     * @param unit timeunit of next array
     * @param times array of arrays storing multiple precounting time levels. Element containts 
     * array with  2-4 elements. 1.element is number of units, 2.element is number of statistics 
     * stored in 1 aggregation document. voluntary 3.element is left range for sliding statistics.
     * voluntary 4. element is right range for sliding statistics. Eg. {{7,4}} with unit set to days,
     * represet week statistics stored in month documents (as contating 4 weeks). {{1,30,3,3}} represents 
     * sliding week stored in month document
     * @param event one event to store
     */
    void saveEvent(TimeUnit unit, int[][] times, Event event);
    
}
