package org.monitoring.queryapi.preaggregation;

import org.monitoring.queryapi.Event;

/**
 * Interface for aggregation recomputing. Make implementation with public fields into which are
 * injected values from DB and recompute method that read fields and update them with event 
 * values.
 * @author Michal Dubravcik
 */
public interface PreaggregateCompute {

    public void recompute(Event event);
    
}
