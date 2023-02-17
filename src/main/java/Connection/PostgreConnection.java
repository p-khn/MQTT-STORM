package Connection;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgreConnection {
    private static final Logger logger = LoggerFactory.getLogger(PostgreConnection.class.getName());
    private final String url = "jdbc:postgresql://172.17.0.1/mqttstormsensdata";
    private final String user = "postgres";
    private final String password = "You May Put Your Password Here!!!";

    public Connection connect(){
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url,user,password);
            conn.setAutoCommit(false);
            logger.info("Connected to Postgresql server successfully");
        }catch (SQLException e){
            e.printStackTrace();
        }

        return conn;
    }

    public void create_table(){

      Connection conn = connect();
      Statement statement = null;

        try {
            statement = conn.createStatement();
            String sql = "CREATE TABLE sensor_data" +
                    "( ID SERIAL PRIMARY KEY NOT NULL," +
                    "sensor_id INT," +
                    "sensor_name TEXT,"+
                    "message_date TEXT," +
                    "value REAL)";
            statement.executeUpdate(sql);
            statement.close();
            conn.close();

        } catch (SQLException e) {
            e.printStackTrace();
            System.err.println( e.getClass().getName()+": "+ e.getMessage() );
            System.exit(0);
        }

        logger.info("Table created successfully");


    }

    public static void main(String[] args) {
        PostgreConnection postgreConnection = new PostgreConnection();
        postgreConnection.create_table();
    }
}
