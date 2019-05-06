package com.wf.option.pricing.kafka;

import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;

import java.util.Properties;

/**
 * Created by hems on 05/05/19.
 */
public class KafkaBoltBuilder {

    Properties configuration;

    KafkaBoltBuilder(Properties configuration) {
        this.configuration = configuration;
    }


    public KafkaBolt<String, String> createKafkaBolt() {

        return new KafkaBolt<String, String>()
                .withProducerProperties(configuration)
                .withTopicSelector(new DefaultTopicSelector(configuration.getProperty(KafkaBolt.TOPIC)))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>("optionName","optionDataWithPrice"));
    }

    public Properties getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Properties configuration) {
        this.configuration = configuration;
    }
}
