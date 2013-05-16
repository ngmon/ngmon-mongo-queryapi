package org.monitoring.queryapi;

import java.util.Date;

/**
 *
 * @author Michal Dubravcik
 */
public class CachePoint extends CacheMatcher {

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
    public static final String ID_TIME = "time";
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
