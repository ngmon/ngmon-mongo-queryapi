package org.monitoring.queryapi;

import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Entity;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Property;
import java.util.Date;
import org.bson.types.ObjectId;

/**
 *
 * @author Michal Dubravcik
 */
@Entity(value = "querytest", noClassnameStored=true)
public class Document {

    @Id
    private ObjectId id;
    private Date date;
    @Embedded
    private DocumentData data;

    public ObjectId getId() {
        return id;
    }

    public void setId(ObjectId id) {
        this.id = id;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public DocumentData getData() {
        return data;
    }

    public void setData(DocumentData data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Document other = (Document) obj;
        if (this.id != other.id && (this.id == null || !this.id.equals(other.id))) {
            return false;
        }
        return true;
    }
    
    
    
}
