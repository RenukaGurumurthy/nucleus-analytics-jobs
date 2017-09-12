package org.gooru.nucleus.consumer.sync.jobs.processors;

import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

public class CopyFrom {
  public static void main(String[] args) {

    Connection con = null;
    PreparedStatement pst = null;
    ResultSet rs = null;
    FileReader fr = null;

    String url = "jdbc:postgresql://localhost/reports_db";
    String user = "reports_user";
    String password = "nucleus";

    try {

        con = DriverManager.getConnection(url, user, password);
       
        CopyManager cm = new CopyManager((BaseConnection) con);

        fr = new FileReader("/tmp/users123.csv");
        cm.copyIn("COPY users FROM STDIN WITH DELIMITER ','", fr);
        
    } catch (SQLException | IOException ex) {
        Logger lgr = Logger.getLogger(CopyFrom.class.getName());
        lgr.log(Level.SEVERE, ex.getMessage(), ex);

    } finally {

        try {
            if (rs != null) {
                rs.close();
            }
            if (pst != null) {
                pst.close();
            }
            if (con != null) {
                con.close();
            }
            if (fr != null) {
                fr.close();
            }

        } catch (SQLException | IOException ex) {
            Logger lgr = Logger.getLogger(CopyFrom.class.getName());
            lgr.log(Level.WARNING, ex.getMessage(), ex);
        }
    }
}

}
