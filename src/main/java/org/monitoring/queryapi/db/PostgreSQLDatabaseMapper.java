package org.monitoring.queryapi.db;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import org.monitoring.queryapi.Event;

/**
 *
 * @author Michal
 */
public class PostgreSQLDatabaseMapper {

    public List<Event> getResult(ResultSet rs) {
        List<Event> result = new LinkedList<Event>();
        try {
            while (rs.next()) {
                Event event = new Event();
                //event.setId(rs.getInt("id"));
                event.setDate(rs.getDate("date"));
                event.setSource(rs.getString("source"));
                event.setValue(rs.getDouble("value"));
                result.add(event);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
        return result;
    }

    void set(PreparedStatement st, Event event) {
        try {
            st.setString(1, event.getSource());            
            st.setTimestamp(2, new java.sql.Timestamp(event.getDate().getTime()));
            st.setDouble(3, event.getValue());
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

    }
}
