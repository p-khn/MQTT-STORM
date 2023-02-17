package Connection;


import com.google.gson.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class InsertDataPostgre {
    private static final Logger logger = LoggerFactory.getLogger(InsertDataPostgre.class.getName());

    public InsertDataPostgre(){

    }

    public void insertData(String message){
        float SENSOR_VALUE;
        try{
            JsonElement jsonmessage = new JsonParser().parse(message);
            if(jsonmessage.isJsonObject()) {
                JsonObject jsonObject = jsonmessage.getAsJsonObject();
                JsonArray jsonArray = jsonObject.get("sensorMessages").getAsJsonArray();
                for (JsonElement sensor : jsonArray){

                    int SENSOR_ID = Integer.parseInt(sensor.getAsJsonObject().get("sensorID").getAsString());
                    String SENSOR_NAME = sensor.getAsJsonObject().get("sensorName").getAsString();
                    String MESSAGE_DATE = sensor.getAsJsonObject().get("messageDate").getAsString();

                    if (sensor.getAsJsonObject().get("sensorName").toString().contains("Temperature")){
                        SENSOR_VALUE =Float.parseFloat(sensor.getAsJsonObject().get("rawData").getAsString());
                    }else {
                        SENSOR_VALUE  = Float.parseFloat(sensor.getAsJsonObject().get("rawData").getAsString().split("%")[0]);
                    }


                    Connection conn = new PostgreConnection().connect();
                    String sql = "INSERT INTO sensor_data (sensor_id,sensor_name,message_date,value)" + "VALUES(?,?,?,?)";
                    try {
                        PreparedStatement pstatement = conn.prepareStatement(sql);
                        pstatement.setInt(1,SENSOR_ID);
                        pstatement.setString(2,SENSOR_NAME);
                        pstatement.setString(3, MESSAGE_DATE);
                        pstatement.setFloat(4,SENSOR_VALUE);

                        int affRows = pstatement.executeUpdate();

                        if (affRows > 0){
                           logger.info("Inserted Successfully");
                        }else{
                            logger.info("Fail to Insert Sensor Data");
                        }

                        pstatement.close();
                        conn.commit();
                        conn.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }

                }

            }else{
                logger.info( "JSON OBJECT - NOT PROPER FORMAT");
            }

        }catch (JsonSyntaxException e){
            e.printStackTrace();
        }



    }
}
