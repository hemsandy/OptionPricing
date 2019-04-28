package com.wf.option.pricing;

import com.wf.option.pricing.model.OptionData;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.jms.JmsMessageProducer;
import org.apache.storm.jms.bolt.JmsBolt;
import org.apache.storm.jms.spout.JmsSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Tuple;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.List;

public class OptionPricingJmsTopology {

	public static void main(String[] args) throws Exception{
		System.out.println("Creating Topology");

		ApplicationContext context = new ClassPathXmlApplicationContext("spring/application-config.xml");

		OptionJMSProvider optionJMSQueueProvider = new OptionJMSProvider(context, "jmsActiveMQFactory", "priceTickerSource");
		OptionJMSProvider optionJMSTopicProvider = new OptionJMSProvider(context, "jmsActiveMQFactory", "optionPriceTopic");

		JmsSpout priceTickerSpout = new JmsSpout();

		OptionJMSTupleProducer tupleProducer = new OptionJMSTupleProducer();
		priceTickerSpout.setJmsProvider(optionJMSQueueProvider);
		priceTickerSpout.setJmsTupleProducer(tupleProducer);
		priceTickerSpout.setJmsAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
		priceTickerSpout.setDistributed(false);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("OPTION_WITH_STOCK_PRICE_SPOUT", priceTickerSpout);
		//Pricing Bolt
		builder.setBolt("OPTION_PRICER_BOLT", new OptionPricerBolt("OPTION_PRICER_BOLT", true));

		//JMS Bolt

		// bolt that subscribes to the intermediate bolt, and publishes to a JMS Topic
		JmsBolt jmsBolt = new JmsBolt();
		jmsBolt.setJmsProvider(optionJMSTopicProvider);

		// Publishes the OptionData jsonString to Topic
		jmsBolt.setJmsMessageProducer(new JmsMessageProducer() {
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


		});

		builder.setBolt("PUBLISHER_BOLT", jmsBolt).shuffleGrouping("OPTION_PRICER_BOLT");


		Config conf = new Config();
		conf.setDebug(true);

		if(args != null && args.length > 0){
			conf.setMaxTaskParallelism(1);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("OPTION_PRICER_TOPOLOGY", conf, builder.createTopology());
		}else{
			conf.setNumWorkers(2);
			StormSubmitter.submitTopology("OPTION_PRICER_TOPOLOGY", conf, builder.createTopology());
		}
	}
}
