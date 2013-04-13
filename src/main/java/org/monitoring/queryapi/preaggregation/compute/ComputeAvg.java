package org.monitoring.queryapi.preaggregation.compute;

import org.monitoring.queryapi.Event;

/**
 *
 * @author Michal Dubravcik
 */
public class ComputeAvg implements Compute, Cloneable {

    public double sum = 0;
    public double count = 0;
    public double avg = 0;
    
    @Override
    public void recompute(Event event) {
        sum += event.getValue();
        count += 1;        
        avg = sum / count;
    }
}
