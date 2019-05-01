package com.wf.option.pricing;

import com.wf.option.pricing.model.OptionData;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.jms.JmsMessageProducer;
import org.apache.storm.jms.bolt.JmsBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.List;

public class OptionPricingJmsTopology {

	private static Logger log = LoggerFactory.getLogger(OptionPricingJmsTopology.class);

	public static String OPTION_WITH_STOCK_PRICE_SPOUT="OPTION_WITH_STOCK_PRICE_SPOUT";
	public static String OPTION_PRICING_BOLT="OPTION_PRICING_BOLT";
	public static String OPTION_TRANSFORMER_BOLT="OPTION_TRANSFORMER_BOLT";
	public static String PUBLISHER_BOLT="PUBLISHER_BOLT";
	public static String OPTION_PRICING_TOPOLOGY="OPTION_PRICING_TOPOLOGY";

	public static void main(String[] args) throws Exception{

		if(args == null || args.length < 1) {
			log.error("USAGE: java ..OptionPricingJmsTopology http://refdataurl true/false");
			System.out.println("USAGE: java ..OptionPricingJmsTopology http://refdataurl true/false");
			System.exit(0);
		}
		log.info("Creating Topology ");

		ApplicationContext context = new ClassPathXmlApplicationContext("spring/application-config.xml");

		OptionJMSProvider optionJMSQueueProvider = new OptionJMSProvider(context, "jmsActiveMQFactory", "priceTickerSource");
		OptionJMSProvider optionJMSTopicProvider = new OptionJMSProvider(context, "jmsActiveMQFactory", "optionPriceTopic");

		//JmsSpout priceTickerSpout = new JmsSpout();
		OptionJmsSpout priceTickerSpout = new OptionJmsSpout(args[0], Boolean.parseBoolean(args[1]));

		OptionJMSTupleProducer tupleProducer = new OptionJMSTupleProducer();
		priceTickerSpout.setJmsProvider(optionJMSQueueProvider);
		priceTickerSpout.setJmsTupleProducer(tupleProducer);
		priceTickerSpout.setJmsAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
		priceTickerSpout.setDistributed(false);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(OPTION_WITH_STOCK_PRICE_SPOUT, priceTickerSpout);
		//Transformer Bolt
		//builder.setBolt(OPTION_TRANSFORMER_BOLT, new OptionDataReaderBolt()).shuffleGrouping(OPTION_WITH_STOCK_PRICE_SPOUT);
		//Pricing Bolt
//		builder.setBolt(OPTION_PRICING_BOLT, new OptionPricerBolt(OPTION_PRICING_BOLT, true), 5)
//				.shuffleGrouping(OPTION_TRANSFORMER_BOLT);
		builder.setBolt(OPTION_PRICING_BOLT, new OptionPricerBolt(OPTION_PRICING_BOLT, true), 5)
				.shuffleGrouping(OPTION_WITH_STOCK_PRICE_SPOUT);

		//JMS Bolt

		//bolt that subscribes to the intermediate bolt, and publishes to a JMS Topic
		//JmsBolt jmsBolt = new JmsBolt();
		//jmsBolt.setJmsProvider(optionJMSTopicProvider);

		// Publishes the OptionData jsonString to Topic
		/*jmsBolt.setJmsMessageProducer(new JmsMessageProducer() {
			@Override
			public Message toMessage(Session session, ITuple iTuple) throws JMSException {
				List<OptionData> optionDataList = (List<OptionData>) ((Tuple)iTuple).getValue(0);
				if(optionDataList != null && !optionDataList.isEmpty()) {
					String jsonString = optionDataList.get(0).toJSONString();
					System.out.println("Sending JMS Message:" + jsonString);
					TextMessage tm = session.createTextMessage(jsonString);
					return tm;
				}else {
					return null;
				}
			}


		});*/

		//builder.setBolt(PUBLISHER_BOLT, jmsBolt);


		Config conf = new Config();
		conf.setDebug(true);

		if(args != null && args.length > 0){
			conf.setMaxTaskParallelism(1);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(OPTION_PRICING_TOPOLOGY, conf, builder.createTopology());
		}else{
			conf.setNumWorkers(2);
			StormSubmitter.submitTopology(OPTION_PRICING_TOPOLOGY, conf, builder.createTopology());
		}
		log.info("Submitted Topology ");
	}
}
