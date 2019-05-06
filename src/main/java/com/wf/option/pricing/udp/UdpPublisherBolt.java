package com.wf.option.pricing.udp;

import com.wf.option.pricing.model.OptionData;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseTickTupleAwareRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Map;
import java.util.Properties;

/**
 * Created by hems on 06/05/19.
 */
public class UdpPublisherBolt extends BaseTickTupleAwareRichBolt {

    protected OutputCollector collector;
    private Logger logger = LoggerFactory.getLogger(UdpPublisherBolt.class);

    private DatagramSocket socket;
    private InetAddress address;
    private int port;
    private Map<String, String> properties;

    private byte[] buf;

    public UdpPublisherBolt(Map<String,String> props) {
        this.properties = props;
    }
    @Override
    protected void process(Tuple tuple) {
        try {
            if(tuple.fieldIndex("optionDataWithPrice")>0) {
                OptionData optionData = (OptionData) tuple.getValueByField("optionDataWithPrice");
                sendPacket(optionData.toJSONString());
            }else {
                logger.info("NOT_AN_OPTION_DATA");
            }
            collector.ack(tuple);
        } catch (Exception e) {
            this.collector.reportError(e);
            this.collector.fail(tuple);
        }
    }

    @Override
    protected void onTickTuple(Tuple tuple) {
        logger.info("TICK_TUPLE");
    }

    public String sendPacket(String msg) throws Exception{
        buf = msg.getBytes();
        DatagramPacket packet
                = new DatagramPacket(buf, buf.length, address, port);
        socket.send(packet);

        return msg;
    }


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try {
            this.collector = outputCollector;
            socket = new DatagramSocket();
            address = InetAddress.getByName((properties.get("udp.host")));
            port = Integer.parseInt(properties.get("udp.port"));
        }catch(Exception e) {
           outputCollector.reportError(e);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
        socket.close();
    }
}
