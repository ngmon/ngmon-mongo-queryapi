package org.monitoring.queryapi;

import java.util.Date;

/**
 *
 * @author Michal Dubravcik
 */
public class Event {

    private Date date;
    private double value;

    public double getValue2() {
        return value2;
    }

    public void setValue2(double value2) {
        this.value2 = value2;
    }
    private double value2;

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
