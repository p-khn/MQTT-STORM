package Mapper;

import org.apache.storm.mqtt.MqttMessage;
import org.apache.storm.mqtt.MqttMessageMapper;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomMessageMapper implements MqttMessageMapper {
    private static final Logger logger = LoggerFactory.getLogger(CustomMessageMapper.class.getName());

    @Override
    public Values toValues(MqttMessage message) {

        String topic = message.getTopic();
        String[] topicElements = topic.split("/");
        String[] payloadElements = new String(message.getMessage()).split("/");

        return new Values(topicElements[0], payloadElements[0]);


    }

    @Override
    public Fields outputFields() {
        return new Fields("topic", "message");
    }

}
