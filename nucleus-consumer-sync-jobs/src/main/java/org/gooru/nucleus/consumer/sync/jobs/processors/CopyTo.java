package org.gooru.nucleus.consumer.sync.jobs.processors;

import java.io.FileWriter;
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

public class CopyTo {
  
  public static void main(String[] args) {

    Connection con = null;
    PreparedStatement pst = null;
    ResultSet rs = null;
    FileWriter fw = null;

    String url = "jdbc:postgresql://localhost/nucleus";
    String user = "nucleus";
    String password = "nucleus";

    try {

        con = DriverManager.getConnection(url, user, password);
       
        CopyManager cm = new CopyManager((BaseConnection) con);

        fw = new FileWriter("friends.txt");
        cm.copyOut("COPY friends TO STDOUT WITH DELIMITER AS '|'", fw);

    } catch (SQLException | IOException ex) {
        Logger lgr = Logger.getLogger(CopyTo.class.getName());
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
            if (fw != null) {
                fw.close();
            }

        } catch (SQLException | IOException ex) {
            Logger lgr = Logger.getLogger(CopyTo.class.getName());
            lgr.log(Level.WARNING, ex.getMessage(), ex);
        }
    }
}

  
}
