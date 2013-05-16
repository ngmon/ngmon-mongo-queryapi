package org.monitoring.queryapi;
/**
 *
 * @author Michal Dubravcik
 */
public class CacheMatcher {

    private String operation;
    private String field;
    private String match;
    private long groupTime;
    
    public CacheMatcher(){        
    }

    public CacheMatcher(String operation, String field, String match, long groupTime) {
        this.operation = operation;
        this.field = field;
        this.match = match;
        this.groupTime = groupTime;
    }
    
    public String getMD5(){
        StringBuilder sb = new  StringBuilder(operation);
        sb.append(field);
        sb.append(match.toString());        
        return MD5.getMD5(sb.toString());
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getMatch() {
        return getMD5();//match;
    }

    public void setMatch(String match) {
        this.match = match;
    }

    public long getGroupTime() {
        return groupTime;
    }

    public void setGroupTime(int groupTime) {
        this.groupTime = groupTime;
    }
}
