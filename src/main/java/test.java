import Connection.PostgreConnection;
import com.google.gson.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class test{

    public static void main(String[] args) {

        float SENSOR_VALUE;
        String message = "{\n" +
                "  \"sensorMessages\": [\n" +
                "    {\n" +
                "      \"sensorID\": 407415,\n" +
                "      \"sensorName\": \"Humidity - 407415-Office\",\n" +
                "      \"applicationID\": 43,\n" +
                "      \"networkID\": 55535,\n" +
                "      \"dataMessageGUID\": \"401696e0-db69-4cd0-9fb4-852522ce7e36\",\n" +
                "      \"state\": 0,\n" +
                "      \"messageDate\": \"2019-05-30 23:14:35\",\n" +
                "      \"rawData\": \"65.01%2c26.13\",\n" +
                "      \"dataType\": \"Percentage|TemperatureData\",\n" +
                "      \"dataValue\": \"65.01|26.13\",\n" +
                "      \"plotValues\": \"65.01|79.034\",\n" +
                "      \"plotLabels\": \"Humidity|Fahrenheit\",\n" +
                "      \"batteryLevel\": 100,\n" +
                "      \"signalStrength\": 78,\n" +
                "      \"pendingChange\": \"True\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"sensorID\": 407410,\n" +
                "      \"sensorName\": \"Current Meter 150 Amp - 407410 Blue\",\n" +
                "      \"applicationID\": 94,\n" +
                "      \"networkID\": 55535,\n" +
                "      \"dataMessageGUID\": \"c23aaf0a-e98f-43f9-bc90-545820e92601\",\n" +
                "      \"state\": 0,\n" +
                "      \"messageDate\": \"2019-05-30 23:20:04\",\n" +
                "      \"rawData\": \"548.47%2c12.57%2c12.73%2c12.42\",\n" +
                "      \"dataType\": \"AmpHours|Amps|Amps|Amps\",\n" +
                "      \"dataValue\": \"548.47|12.57|12.73|12.42\",\n" +
                "      \"plotValues\": \"548.47|12.57|12.73|12.42\",\n" +
                "      \"plotLabels\": \"Ah|AvgCurrent|MaxCurrent|MinCurrent\",\n" +
                "      \"batteryLevel\": 100,\n" +
                "      \"signalStrength\": 62,\n" +
                "      \"pendingChange\": \"True\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"sensorID\": 407411,\n" +
                "      \"sensorName\": \"Current Meter 150 Amp - 407411 (Y)\",\n" +
                "      \"applicationID\": 94,\n" +
                "      \"networkID\": 55535,\n" +
                "      \"dataMessageGUID\": \"cc4f8213-2992-47f1-b6cb-16e7bed6c9cc\",\n" +
                "      \"state\": 0,\n" +
                "      \"messageDate\": \"2019-05-30 23:20:44\",\n" +
                "      \"rawData\": \"485.02%2c9.18%2c11.96%2c6.42\",\n" +
                "      \"dataType\": \"AmpHours|Amps|Amps|Amps\",\n" +
                "      \"dataValue\": \"485.02|9.18|11.96|6.42\",\n" +
                "      \"plotValues\": \"485.02|9.18|11.96|6.42\",\n" +
                "      \"plotLabels\": \"Amp Hours|AvgCurrent|MaxCurrent|MinCurrent\",\n" +
                "      \"batteryLevel\": 100,\n" +
                "      \"signalStrength\": 64,\n" +
                "      \"pendingChange\": \"True\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"sensorID\": 167361,\n" +
                "      \"sensorName\": \"Temperature - 167361-Office\",\n" +
                "      \"applicationID\": 2,\n" +
                "      \"networkID\": 55535,\n" +
                "      \"dataMessageGUID\": \"dbb09013-dd3e-4e40-a6c6-9679b6c80645\",\n" +
                "      \"state\": 16,\n" +
                "      \"messageDate\": \"2019-05-30 23:21:23\",\n" +
                "      \"rawData\": 26.3,\n" +
                "      \"dataType\": \"TemperatureData\",\n" +
                "      \"dataValue\": 26.3,\n" +
                "      \"plotValues\": 79.34,\n" +
                "      \"plotLabels\": \"Fahrenheit\",\n" +
                "      \"batteryLevel\": 100,\n" +
                "      \"signalStrength\": 61,\n" +
                "      \"pendingChange\": \"True\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"sensorID\": 407415,\n" +
                "      \"sensorName\": \"Humidity - 407415-Office\",\n" +
                "      \"applicationID\": 43,\n" +
                "      \"networkID\": 55535,\n" +
                "      \"dataMessageGUID\": \"401696e0-db69-4cd0-9fb4-852522ce7e36\",\n" +
                "      \"state\": 0,\n" +
                "      \"messageDate\": \"2019-05-30 23:14:35\",\n" +
                "      \"rawData\": \"65.01%2c26.13\",\n" +
                "      \"dataType\": \"Percentage|TemperatureData\",\n" +
                "      \"dataValue\": \"65.01|26.13\",\n" +
                "      \"plotValues\": \"65.01|79.034\",\n" +
                "      \"plotLabels\": \"Humidity|Fahrenheit\",\n" +
                "      \"batteryLevel\": 100,\n" +
                "      \"signalStrength\": 78,\n" +
                "      \"pendingChange\": \"True\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"sensorID\": 407410,\n" +
                "      \"sensorName\": \"Current Meter 150 Amp - 407410 Blue\",\n" +
                "      \"applicationID\": 94,\n" +
                "      \"networkID\": 55535,\n" +
                "      \"dataMessageGUID\": \"c23aaf0a-e98f-43f9-bc90-545820e92601\",\n" +
                "      \"state\": 0,\n" +
                "      \"messageDate\": \"2019-05-30 23:20:04\",\n" +
                "      \"rawData\": \"548.47%2c12.57%2c12.73%2c12.42\",\n" +
                "      \"dataType\": \"AmpHours|Amps|Amps|Amps\",\n" +
                "      \"dataValue\": \"548.47|12.57|12.73|12.42\",\n" +
                "      \"plotValues\": \"548.47|12.57|12.73|12.42\",\n" +
                "      \"plotLabels\": \"Ah|AvgCurrent|MaxCurrent|MinCurrent\",\n" +
                "      \"batteryLevel\": 100,\n" +
                "      \"signalStrength\": 62,\n" +
                "      \"pendingChange\": \"True\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"sensorID\": 407411,\n" +
                "      \"sensorName\": \"Current Meter 150 Amp - 407411 (Y)\",\n" +
                "      \"applicationID\": 94,\n" +
                "      \"networkID\": 55535,\n" +
                "      \"dataMessageGUID\": \"cc4f8213-2992-47f1-b6cb-16e7bed6c9cc\",\n" +
                "      \"state\": 0,\n" +
                "      \"messageDate\": \"2019-05-30 23:20:44\",\n" +
                "      \"rawData\": \"485.02%2c9.18%2c11.96%2c6.42\",\n" +
                "      \"dataType\": \"AmpHours|Amps|Amps|Amps\",\n" +
                "      \"dataValue\": \"485.02|9.18|11.96|6.42\",\n" +
                "      \"plotValues\": \"485.02|9.18|11.96|6.42\",\n" +
                "      \"plotLabels\": \"Amp Hours|AvgCurrent|MaxCurrent|MinCurrent\",\n" +
                "      \"batteryLevel\": 100,\n" +
                "      \"signalStrength\": 64,\n" +
                "      \"pendingChange\": \"True\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"sensorID\": 167361,\n" +
                "      \"sensorName\": \"Temperature - 167361-Office\",\n" +
                "      \"applicationID\": 2,\n" +
                "      \"networkID\": 55535,\n" +
                "      \"dataMessageGUID\": \"dbb09013-dd3e-4e40-a6c6-9679b6c80645\",\n" +
                "      \"state\": 16,\n" +
                "      \"messageDate\": \"2019-05-30 23:21:23\",\n" +
                "      \"rawData\": 26.3,\n" +
                "      \"dataType\": \"TemperatureData\",\n" +
                "      \"dataValue\": 26.3,\n" +
                "      \"plotValues\": 79.34,\n" +
                "      \"plotLabels\": \"Fahrenheit\",\n" +
                "      \"batteryLevel\": 100,\n" +
                "      \"signalStrength\": 61,\n" +
                "      \"pendingChange\": \"True\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";
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
                            SENSOR_VALUE = Float.parseFloat(sensor.getAsJsonObject().get("rawData").getAsString());
                            System.out.println(sensor.getAsJsonObject().get("rawData").getAsString());
                        }else {
                            SENSOR_VALUE  = Float.parseFloat(sensor.getAsJsonObject().get("rawData").getAsString().split("%")[0]);
                            System.out.println(sensor.getAsJsonObject().get("rawData").getAsString().split("%")[0]);
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
                                System.out.println("inserted");
                            }else{
                                System.out.println("not inserted");
                            }

                            pstatement.close();
                            conn.commit();
                            conn.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }

                    }

            }else{
                System.out.println("NOT PROPER FORMAT");
            }

        }catch (JsonSyntaxException e){
            e.printStackTrace();
        }

    }
}
