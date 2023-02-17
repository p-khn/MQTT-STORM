package Bolts;

import Connection.InsertDataPostgre;
import com.google.gson.*;
import com.opencsv.CSVWriter;
import org.apache.storm.mqtt.MqttTupleMapper;
import org.apache.storm.mqtt.bolt.MqttBolt;
import org.apache.storm.mqtt.common.MqttOptions;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;

public class MqttBolt1 extends MqttBolt {

    private static final Logger logger = LoggerFactory.getLogger(MqttBolt1.class.getName());
    private OutputCollector collector;

    // keep in mind to adjust csv file location base on your rules
    private static File file = new File("/apache-storm-1.0.6/sensordata.csv");


    public MqttBolt1(MqttOptions options, MqttTupleMapper mapper) {
        super(options, mapper);

    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
          logger.info("************"+input.getStringByField("topic"));
          logger.info("-------------"+input.getStringByField("message"));
          String message = input.getStringByField("message");
          String[] senData = new String[6];
          senData[0] = input.getStringByField("topic");


        try{
            JsonElement jsonmessage = new JsonParser().parse(message);
            if(jsonmessage.isJsonObject()) {
                JsonObject jsonObject = jsonmessage.getAsJsonObject();
                JsonArray jsonArray = jsonObject.get("sensorMessages").getAsJsonArray();
                for (JsonElement sensor : jsonArray){
                    String type = sensor.getAsJsonObject().get("sensorName").toString();

                    if(type.contains("Blue")) {
                        senData[1] = sensor.getAsJsonObject().get("rawData").getAsString().split("%")[0];
                    }else if (type.contains("(Y")){
                        senData[2] = sensor.getAsJsonObject().get("rawData").getAsString().split("%")[0];
                    }else if (type.contains("Temperature")){
                        senData[3] = sensor.getAsJsonObject().get("rawData").getAsString();
                    }else if (type.contains("Humidity")){
                        senData[4] = sensor.getAsJsonObject().get("rawData").getAsString().split("%")[0];
                    }else if (type.contains("Vibration")){
                        senData[5] = sensor.getAsJsonObject().get("rawData").getAsString().split("%")[0];
                    }else if (type.contains("Open")){
                        senData[1] = null;
                    }
                }
            }else{
                logger.info( "JSON OBJECT - NOT PROPER FORMAT");
            }
        }catch (JsonSyntaxException e){
            e.printStackTrace();
        }

        try {
            FileReader fileReader = new FileReader(file);
            int i = fileReader.read();
            if (i == -1) {
                FileWriter outputfile = new FileWriter(file, true);
                CSVWriter writer = new CSVWriter(outputfile, ',',
                        CSVWriter.NO_QUOTE_CHARACTER,
                        CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                        CSVWriter.DEFAULT_LINE_END);
                String[] header = {"topic","temperature","humidity","currentMeterBlue","currentMeterY","vibrationMeter"};
                writer.writeNext(header);
                if(senData != null && senData[1] != null){
                    writer.writeNext(senData);
                }
                writer.close();
            } else {
                FileWriter outputfile = new FileWriter(file, true);
                CSVWriter writer = new CSVWriter(outputfile, ',',
                        CSVWriter.NO_QUOTE_CHARACTER,
                        CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                        CSVWriter.DEFAULT_LINE_END);
                if(senData != null && senData[1] != null){
                    writer.writeNext(senData);
                }
                writer.close();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Insert Data to PostgreSQL
        InsertDataPostgre insertDataPostgre = new InsertDataPostgre();
        insertDataPostgre.insertData(message);

        collector.ack(input);
    }


}

