package barac.storm;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import com.google.common.collect.Maps;

public class PrimeNumberTopology {
	private static String zkhost, inputTopic, KafkaBroker, consumerGroup;

	static Map<String, String> HBConfig = Maps.newHashMap();

	public static void Intialize(String arg) {
		Properties prop = new Properties();
		InputStream input = null;

		try {
			System.out.println("Loading Configuration File for setting up input");
			input = new FileInputStream(arg);
			prop.load(input);

			// Kafka config

			zkhost = prop.getProperty("zkhost");
			inputTopic = prop.getProperty("inputTopic");

			KafkaBroker = prop.getProperty("KafkaBroker");
			consumerGroup = prop.getProperty("consumerGroup");

		

		} catch (FileNotFoundException ex) {
			System.out.println("Error While loading configuration file" + ex.getMessage());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					System.out.println("Error Closing input stream");

				}
			}
		}

	}

	public static void main(String[] args) {
		Intialize(args[0]);
		System.out.println("Successfully loaded Configuration");

		/*
		 * ZkHosts zkHosts = new ZkHosts(zkhost); SpoutConfig kafkaConfig = new
		 * SpoutConfig(zkHosts, inputTopic, "/" + inputTopic, consumerGroup);
		 * kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		 */
		
        // kafka config
		BrokerHosts hosts = new ZkHosts(zkhost);
		SpoutConfig spoutConfig = new SpoutConfig(hosts, inputTopic, "/" + KafkaBroker, consumerGroup);
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
	//	spoutConfig.forceFromStart = false;
		spoutConfig.ignoreZkOffsets= true;			
		spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();	
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		System.out.println("Creating Storm Topology");
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("KafkaSpout", kafkaSpout, 1);
		builder.setBolt("prime", new PrimeNumberBolt(), 1).shuffleGrouping("KafkaSpout");

		Config conf = new Config();
		conf.setNumWorkers(1);
		System.out.println("Submiting  topology to storm cluster");
		//conf.setMaxSpoutPending(5000);
		try {
			StormSubmitter.submitTopology("primenumbertopology", conf, builder.createTopology());
		} catch (AlreadyAliveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (AuthorizationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
