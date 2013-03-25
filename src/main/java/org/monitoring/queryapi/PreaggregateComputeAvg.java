package org.monitoring.queryapi;

/**
 *
 * @author Michal Dubravcik
 */
public class PreaggregateComputeAvg implements PreaggregateCompute, Cloneable {

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
