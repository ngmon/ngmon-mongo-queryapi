/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.monitoring.queryapi;

/**
 * Interface for aggregation recomputing. Make implementation with public fields into which are
 * injected values from DB and recompute method that read fields and update them with event 
 * values.
 * @author Michal Dubravcik
 */
public interface PreaggregateCompute {

    public void recompute(Event event);
    
}
