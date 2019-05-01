package com.wf.option.pricing;


import java.util.List;
import java.util.Map;

import com.wf.option.pricing.model.OptionData;
import org.apache.storm.Config;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OptionDataReaderBolt extends BaseBasicBolt {

    private static Logger logger = LoggerFactory.getLogger(OptionDataReaderBolt.class);
    private Integer emitFrequency;


    public OptionDataReaderBolt() {
        emitFrequency = 5;
    }

    public OptionDataReaderBolt(Integer emitFrequency) {
        this.emitFrequency = emitFrequency;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequency);
        return conf;
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        List<Object> values = tuple.getValues();

        logger.info("Tuple Length: {}" ,tuple.getFields().size());
        for( int i=0;i<tuple.getFields().size(); i++) {
            logger.info("Tuple Fields: {}, value: {}", tuple.getFields().get(i), values.get(i));
        }
        if(values.size() == 2) {
            OptionData optionData = OptionData.fromJsonString(
                    (String) values.get(tuple.fieldIndex("optionPriceJson")));
            double underlyingTickPrice = (Double) values.get(tuple.fieldIndex("underlyingPrice"));
            collector.emit(new Values(optionData, underlyingTickPrice));
        }else{
            //TODO: need to investigate
            logger.info("need to investigate..");

        }


    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("optionPriceJson", "underlyingPrice"));
    }

}
