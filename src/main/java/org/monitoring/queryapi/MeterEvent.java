package org.monitoring.queryapi;

import java.util.Date;

/**
 *
 * @author Michal Dubravcik
 */
public class MeterEvent {

    private Date date;
    private double value;

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }
    
}
