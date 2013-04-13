package org.monitoring.queryapi.preaggregation.compute;

import org.monitoring.queryapi.Event;

/**
 *
 * @author Michal Dubravcik
 */
public class ComputeMaxMin implements Compute {

    public double max = 0;
    public double min = 0;
    public double diff = 0;
    
    @Override
    public void recompute(Event event) {
        if (event.getValue() > max) {
            max = event.getValue();
        }
        if (event.getValue() < min) {
            min = event.getValue();
        }
        diff = max - min;
    }

}
