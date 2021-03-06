package com.wf.option.pricing;

import com.wf.option.pricing.kafka.KafkaBoltBuilder;
import com.wf.option.pricing.model.OptionData;
import com.wf.option.pricing.redis.OptionPriceRedisBolt;
import com.wf.option.pricing.redis.RedisBoltBuilder;
import com.wf.option.pricing.udp.UdpPublisherBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.jms.JmsMessageProducer;
import org.apache.storm.jms.bolt.JmsBolt;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;

public class OptionPricingJmsTopology {

	private static Logger log = LoggerFactory.getLogger(OptionPricingJmsTopology.class);

	public static String OPTION_WITH_STOCK_PRICE_SPOUT="OPTION_WITH_STOCK_PRICE_SPOUT";
	public static String OPTION_PRICING_BOLT="OPTION_PRICING_BOLT";
	public static String OPTION_TRANSFORMER_BOLT="OPTION_TRANSFORMER_BOLT";
	public static String PUBLISHER_BOLT="PUBLISHER_BOLT";
	public static String OPTION_PRICING_TOPOLOGY="OPTION_PRICING_TOPOLOGY";

	public static void main(String[] args) throws Exception{

		if(args == null || args.length < 1) {
			log.error("USAGE: java ..OptionPricingJmsTopology http://refdataurl local/remote jms/redis/kafka ");
			System.out.println("USAGE: java ..OptionPricingJmsTopology http://refdataurl local/remote jms/redis/kafka");
			System.exit(0);
		}
		log.info("Creating Topology {}", OPTION_PRICING_TOPOLOGY);

		ApplicationContext context = new ClassPathXmlApplicationContext("spring/application-config.xml");
		//Configuration
		Config conf = new Config();
		//conf.setDebug(false);
		/* - System Properties
			-Dnum.workers
			-Dnum.spouts
			-Dnum.pricers
			-Dnum.publishers
		 */
		int workerCount =  2;
		int spoutCount = 1;
		int pricerCount = 5;
		int publisherCount = 5;

		String tmpVal= System.getProperty("num.workers");
		if(tmpVal != null && !tmpVal.trim().isEmpty()) {
			workerCount = Integer.parseInt(tmpVal);
		}

		tmpVal= System.getProperty("num.spouts");
		if(tmpVal != null && !tmpVal.trim().isEmpty()) {
			spoutCount = Integer.parseInt(tmpVal);
		}
		tmpVal= System.getProperty("num.pricers");
		if(tmpVal != null && !tmpVal.trim().isEmpty()) {
			pricerCount = Integer.parseInt(tmpVal);
		}
		tmpVal= System.getProperty("num.publishers");
		if(tmpVal != null && !tmpVal.trim().isEmpty()) {
			publisherCount = Integer.parseInt(tmpVal);
		}

		log.info("Parameters: Workers -{}, Spouts -{}, Pricers-{}, Publishers-{}", workerCount,spoutCount,pricerCount,publisherCount);


		OptionJMSProvider optionJMSQueueProvider = new OptionJMSProvider(context, "jmsActiveMQFactory", "priceTickerSource");
		OptionJMSProvider optionJMSTopicProvider = new OptionJMSProvider(context, "jmsActiveMQFactory", "optionPriceTopic");

		//JmsSpout priceTickerSpout = new JmsSpout();
		//set second param is false always.. Boolean.parseBoolean(args[1])
		OptionJmsSpout priceTickerSpout = new OptionJmsSpout(args[0], false);

		OptionJMSTupleProducer tupleProducer = new OptionJMSTupleProducer();
		priceTickerSpout.setJmsProvider(optionJMSQueueProvider);
		priceTickerSpout.setJmsTupleProducer(tupleProducer);
		priceTickerSpout.setJmsAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
		priceTickerSpout.setDistributed(false);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(OPTION_WITH_STOCK_PRICE_SPOUT, priceTickerSpout,spoutCount);
		//Transformer Bolt
//		builder.setBolt(OPTION_TRANSFORMER_BOLT, new OptionDataReaderBolt(),spoutCount)
//				.shuffleGrouping(OPTION_WITH_STOCK_PRICE_SPOUT);
		//Pricing Bolt
		builder.setBolt(OPTION_PRICING_BOLT, new OptionPricerBolt(OPTION_PRICING_BOLT, true), pricerCount)
				//.shuffleGrouping(OPTION_TRANSFORMER_BOLT);
				.shuffleGrouping(OPTION_WITH_STOCK_PRICE_SPOUT);
		String output = "jms";
		if(args.length >2 && args[2] != null) {
			output = args[2];
		}
		if(output.equalsIgnoreCase("redis")) {
			//Redis Bolt
			RedisBoltBuilder redisBoltBuilder = ((RedisBoltBuilder) context.getBean("redisBuilder"));
			//OptionPriceRedisBolt redisStoreBolt = redisBoltBuilder.createInstanceCustom();
			RedisStoreBolt redisStoreBolt = redisBoltBuilder.createInstance();

			builder.setBolt(PUBLISHER_BOLT, redisStoreBolt, publisherCount)
					.shuffleGrouping(OPTION_PRICING_BOLT);//, new Fields("optionName")

		}else if(output.equalsIgnoreCase("jms")){
			//JMS Bolt

			//bolt that subscribes to the intermediate bolt, and publishes to a JMS Topic
			JmsBolt jmsBolt = new JmsBolt();
			jmsBolt.setJmsProvider(optionJMSTopicProvider);

			// Publishes the OptionData jsonString to Topic
			jmsBolt.setJmsMessageProducer(new JmsMessageProducer() {
				@Override
				public Message toMessage(Session session, ITuple iTuple) throws JMSException {
					TextMessage tm = null;
					try {
						OptionData optionData = (OptionData) iTuple.getValue(0);
						String jsonString = optionData.toJSONString();
						//log.info("Sending JMS Message:" + jsonString);
						tm = session.createTextMessage(jsonString);
					} catch (Exception e) {
						log.error("Exception .. in Sending JMS Message to Topic");
					}
					return tm;
				}
			});
			builder.setBolt(PUBLISHER_BOLT, jmsBolt, publisherCount)
					.fieldsGrouping(OPTION_PRICING_BOLT, new Fields("optionName"));

		}else if(output.equalsIgnoreCase("kafka")) {

			KafkaBoltBuilder kafkaBoltBuilder = (KafkaBoltBuilder)context.getBean("kafkaBoltBuilder");
			KafkaBolt<String, String> kafkaBolt = kafkaBoltBuilder.createKafkaBolt();
			builder.setBolt(PUBLISHER_BOLT, kafkaBolt, publisherCount)
					.fieldsGrouping(OPTION_PRICING_BOLT, new Fields("optionName"));

		}else if(output.equalsIgnoreCase("udp")) {

			UdpPublisherBolt udpPublisherBolt = ((UdpPublisherBolt) context.getBean("udpPublisherBolt"));
			builder.setBolt(PUBLISHER_BOLT, udpPublisherBolt, publisherCount)
					.shuffleGrouping(OPTION_PRICING_BOLT);
		}


		if(args[1].equalsIgnoreCase("local")){
			conf.setMaxTaskParallelism(1);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(OPTION_PRICING_TOPOLOGY, conf, builder.createTopology());
		}else{
			conf.setNumWorkers(workerCount);
			StormSubmitter.submitTopology(OPTION_PRICING_TOPOLOGY, conf, builder.createTopology());
		}
		log.info("Submitted Topology {}", OPTION_PRICING_TOPOLOGY);
	}
}
