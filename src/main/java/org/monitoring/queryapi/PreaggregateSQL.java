package org.monitoring.queryapi;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.monitoring.queryapi.db.PostgreSQLDatabase;

/**
 *
 * @author Michal Dubravcik
 */
public class PreaggregateSQL implements Preaggregate{
    String colName = "aggregate";
    PostgreSQLDatabase postgre = new PostgreSQLDatabase();

    public PreaggregateSQL() {
    }
    
    public PreaggregateSQL(String colName) {
        this.colName = colName;
    }
    
    public void saveEvent(TimeUnit unit, int[][] times, Event event) {
        Long fieldTime;
        int i = 0;
        postgre.save(event);

        while (i < times.length) {
            int timeActual = times[i][0];
            int timeNext = times[i][1];
            int rangeLeft = 0;
            if (times[i].length > 2) {
                rangeLeft = times[i][2];
            }
            int rangeRight = 0;
            if (times[i].length > 3) {
                rangeRight = times[i][3];
            }
            i++;

            String table = colName + timeActual;
            
            for (int k = -rangeLeft; k <= rangeRight; k++) {
                long difference = unit.toMillis(timeActual * k);
                long eventDateLocal = event.getDate().getTime() + difference;

                long eventDate = eventDateLocal - eventDateLocal % unit.toMillis(timeNext);

                fieldTime = eventDateLocal % unit.toMillis(timeNext) / unit.toMillis(timeActual);
                String fieldTimeString = fieldTime.toString();

                postgre.updateAggregate(timeActual, timeNext, new Date(eventDate), fieldTimeString, event);
            }
        }
    }
}

