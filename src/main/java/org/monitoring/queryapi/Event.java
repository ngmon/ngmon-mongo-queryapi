package org.monitoring.queryapi;

import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import java.util.Date;
import org.bson.types.ObjectId;

/**
 *
 * @author Michal Dubravcik
 */
@Entity(value = "aggregate", noClassnameStored=true)
public class Event {

    @Id
    private ObjectId id;
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
