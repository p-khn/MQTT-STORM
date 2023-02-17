package Topology;

import Bolts.MqttBolt1;
import Mapper.CustomMessageMapper;
import Mapper.CustomTupleMapper;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.mqtt.MqttLogger;
import org.apache.storm.mqtt.common.MqttOptions;
import org.apache.storm.mqtt.spout.MqttSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Arrays;
import java.util.Properties;

public class MqttTopology {

    private static final Logger logger = LoggerFactory.getLogger(MqttTopology.class.getName());
    private static final String TEST_TOPIC = "sensordata";
    private static URI uri = null;
//    private static Session session = null;

    public static void main(String[] args) throws Exception{

        int lport;
        String rhost;
        int rport;
        String host = "root@<Server IP>";
        try{
            JSch jsch=new JSch();

            String user=host.substring(0, host.indexOf('@'));
            host=host.substring(host.indexOf('@')+1);

            Session session=jsch.getSession(user, host, 22);
            String addr="1883:<Server IP>:1883";

            lport=Integer.parseInt(addr.substring(0, addr.indexOf(':')));
            addr=addr.substring(addr.indexOf(':')+1);
            rhost=addr.substring(0, addr.indexOf(':'));
            rport=Integer.parseInt(addr.substring(addr.indexOf(':')+1));

            session.setPassword("PASSWORD HERE");
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect();

            int assinged_port=session.setPortForwardingL(lport, rhost, rport);
            String local_port = String.valueOf(assinged_port);
            System.out.println("localhost:"+assinged_port+" -> "+rhost+":"+rport);
            uri = new URI("tcp://" + "127.0.0.1" + ":" + local_port);
        }catch (Exception e){
            e.printStackTrace();
        }


        MQTT client = new MQTT();
            client.setTracer(new MqttLogger());

//            client.setHost("tcp://127.0.0.1:1883");
            client.setUserName("USERNAME HERE");
            client.setPassword("PASSWORD HERE");
            client.setClientId("MQTTSubscriber");
            client.setConnectAttemptsMax(1);
            client.setCleanSession(true);
            BlockingConnection connection = client.blockingConnection();
            connection.connect();

            Topic[] topics = { new Topic("sensordata", QoS.AT_LEAST_ONCE) };
            byte[] qoses = connection.subscribe(topics);

            Config config = new Config();
            config.setDebug(true);
            config.setMaxTaskParallelism(5);

            try {

//                LocalCluster cluster = new LocalCluster();
//                cluster.submitTopology("test", config, buildMqttTopology());
                StormSubmitter.submitTopologyWithProgressBar("PKHN_MQTT_TP",config,buildMqttTopology());
                logger.info("topology started");
                while (!buildMqttTopology().is_set_spouts()) {
                    Thread.sleep(500);
                }
            }
            catch (Exception e){
                throw new IllegalStateException("Could not create topology!!!!:((");


            }


        }

    public static StormTopology buildMqttTopology() {
        TopologyBuilder builder = new TopologyBuilder();
        MqttOptions options = new MqttOptions();
        options.setUrl("tcp://<Server IP>:1883");
        options.setUserName("USERNAME HERE");
        options.setPassword("PASSWORD HERE");
        options.setTopics(Arrays.asList(TEST_TOPIC));
        options.setCleanConnection(true);
        MqttSpout spout = new MqttSpout(new CustomMessageMapper(), options);
        CustomTupleMapper customTupleMapper = new CustomTupleMapper();

        MqttBolt1 bolt = new MqttBolt1(options, customTupleMapper);

        builder.setSpout("mqtt-spout", spout);
        builder.setBolt("log-bolt", bolt).shuffleGrouping("mqtt-spout");

        return builder.createTopology();

    }

}
