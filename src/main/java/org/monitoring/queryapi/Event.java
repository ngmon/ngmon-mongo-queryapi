package org.monitoring.queryapi;

import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import java.util.Date;
import org.bson.types.ObjectId;

/**
 * Basic Event to be stored into DB. 
 * Values are subjects of statistical functions of Query or Preaggregation
 * 
 * @author Michal Dubravcik
 */
@Entity(value = "aggregate", noClassnameStored=true)
public class Event {

    @Id
    private ObjectId id;
    private Date date;
    private double value;
    private String source;

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }


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
