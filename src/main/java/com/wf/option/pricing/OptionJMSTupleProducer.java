package com.wf.option.pricing;

import com.wf.option.pricing.model.OptionData;
import org.apache.storm.jms.JmsTupleProducer;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

/**
 * Created by hems on 19/04/19.
 */
public class OptionJMSTupleProducer implements JmsTupleProducer {

    public Values toTuple(Message msg) throws JMSException {
        if(msg instanceof TextMessage){
            String json = ((TextMessage) msg).getText();
            return new Values(json);
        } else {
            return null;
        }
    }

    public Values toTuple(OptionData optionData, String optionName) {
        return new Values(optionData,optionName );
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("optionData","optionName"));
    }
}
