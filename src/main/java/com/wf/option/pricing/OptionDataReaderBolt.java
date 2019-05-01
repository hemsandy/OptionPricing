package com.wf.option.pricing;


import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import com.google.gson.JsonObject;

public class OptionDataReaderBolt extends BaseBasicBolt {

    private Integer emitFrequency;
    JsonObject jsonObj;
    Double underlyingTickPrice;

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
        List<Object> data = tuple.getValues();
        jsonObj = (JsonObject) data.get(tuple.fieldIndex("optionPriceJson"));
        underlyingTickPrice = (Double) data.get(tuple.fieldIndex("underlyingPrice"));
        collector.emit(new Values(jsonObj, underlyingTickPrice));

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("optionPriceJson", "underlyingPrice"));
    }

}
