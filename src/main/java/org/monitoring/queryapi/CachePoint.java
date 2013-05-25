package org.monitoring.queryapi;

import java.util.Date;

/**
 * Cache point is representation of boundary point (start or end) of time interval in which is 
 * something already stored in cache
 * 
 * @author Michal Dubravcik
 */
public class CachePoint extends CacheMatcher {

    /**
     * Flag is cache point option representing start or end of time interval.
     * None flag is used only in situation when nothing is saved in cache, for representing state
     * nothing was found
     */
    public enum Flag {

        START(0),
        END(1),
        NONE(3);
        int flag;

        private Flag(int flag) {
            this.flag = flag;
        }

        public int get() {
            return flag;
        }
    }
    public static final String ID_TIME = "t";
    private Flag flag;
    private Date date;

    public CachePoint(){
        super();
    };
    
    public CachePoint(Date date, String operation, String field, String match, CachePoint.Flag flag, long groupTime) {
        super(operation, field, match, groupTime);
        this.date = date;
        this.flag = flag;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public Flag getFlag() {
        return flag;
    }

    public void setFlag(Flag flag) {
        this.flag = flag;
    }
}
