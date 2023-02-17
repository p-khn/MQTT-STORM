package Mapper;

import org.apache.storm.mqtt.MqttMessage;
import org.apache.storm.mqtt.MqttTupleMapper;
import org.apache.storm.tuple.ITuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CustomTupleMapper implements MqttTupleMapper {

    private static final Logger logger = LoggerFactory.getLogger(CustomTupleMapper.class.getName());
        private static final String RESULT_PAYLOAD = "Storm MQTT Spout";

    public MqttMessage toMessage(ITuple tuple) {
        String topic = tuple.getStringByField("topic");
        byte[] payload = tuple.getStringByField("message").getBytes();

        return new MqttMessage(topic, payload);
    }
}

