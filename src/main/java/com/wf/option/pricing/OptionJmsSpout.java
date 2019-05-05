package com.wf.option.pricing;



import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.storm.jms.JmsProvider;
import org.apache.storm.jms.JmsTupleProducer;
import org.apache.storm.jms.spout.JmsMessageID;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.wf.option.pricing.model.OptionData;

/**
 * A Storm <code>Spout</code> implementation that listens to a JMS topic or queue
 * and outputs tuples based on the messages it receives.
 * <p/>
 * <code>JmsSpout</code> instances rely on <code>JmsProducer</code> implementations
 * to obtain the JMS <code>ConnectionFactory</code> and <code>Destination</code> objects
 * necessary to connect to a JMS topic/queue.
 * <p/>
 * When a <code>JmsSpout</code> receives a JMS message, it delegates to an
 * internal <code>JmsTupleProducer</code> instance to create a Storm tuple from the
 * incoming message.
 * <p/>
 * Typically, developers will supply a custom <code>JmsTupleProducer</code> implementation
 * appropriate for the expected message content.
 *
 * @author P. Taylor Goetz
 *
 */
@SuppressWarnings("serial")
public class OptionJmsSpout extends BaseRichSpout implements MessageListener {
    private static final Logger LOG = LoggerFactory.getLogger(OptionJmsSpout.class);

    // JMS options
    private int jmsAcknowledgeMode = Session.AUTO_ACKNOWLEDGE;

    private boolean distributed = true;

    private JmsTupleProducer tupleProducer;

    private JmsProvider jmsProvider;

    private LinkedBlockingQueue<Message> queue;
    private TreeSet<JmsMessageID> toCommit;
    private HashMap<JmsMessageID, Message> pendingMessages;
    private long messageSequence = 0;

    private SpoutOutputCollector collector;

    private transient Connection connection;
    private transient Session session;

    private boolean hasFailures = false;
    public final Serializable recoveryMutex = "RECOVERY_MUTEX";
    private Timer recoveryTimer = null;
    private long recoveryPeriod = -1; // default to disabled

    private String refDataUrl;
    private boolean readAll;
    private static int count=0;

    private Map<String, List<OptionData>> optionDataMap;

    public OptionJmsSpout(String refDataUrl, boolean readAll) {
        this.refDataUrl = refDataUrl;
        this.readAll = readAll;
        if(readAll) {
            List<OptionData> optionDataList = readFromUrl(refDataUrl);
            optionDataMap = optionDataList.parallelStream().collect(Collectors.groupingBy(OptionData::getStockName));
        }
    }


    /**
     * Sets the JMS Session acknowledgement mode for the JMS seesion associated with this spout.
     * <p/>
     * Possible values:
     * <ul>
     * <li>javax.jms.Session.AUTO_ACKNOWLEDGE</li>
     * <li>javax.jms.Session.CLIENT_ACKNOWLEDGE</li>
     * <li>javax.jms.Session.DUPS_OK_ACKNOWLEDGE</li>
     * </ul>
     * @param mode JMS Session Acknowledgement mode
     * @throws IllegalArgumentException if the mode is not recognized.
     */
    public void setJmsAcknowledgeMode(int mode){
        switch (mode) {
            case Session.AUTO_ACKNOWLEDGE:
            case Session.CLIENT_ACKNOWLEDGE:
            case Session.DUPS_OK_ACKNOWLEDGE:
                break;
            default:
                throw new IllegalArgumentException("Unknown Acknowledge mode: " + mode + " (See javax.jms.Session for valid values)");

        }
        this.jmsAcknowledgeMode = mode;
    }

    /**
     * Returns the JMS Session acknowledgement mode for the JMS seesion associated with this spout.
     * @return
     */
    public int getJmsAcknowledgeMode(){
        return this.jmsAcknowledgeMode;
    }

    /**
     * Set the <code>JmsProvider</code>
     * implementation that this Spout will use to connect to
     * a JMS <code>javax.jms.Desination</code>
     *
     * @param provider
     */
    public void setJmsProvider(JmsProvider provider){
        this.jmsProvider = provider;
    }
    /**
     * Set the <code>JmsTupleProducer</code>
     * implementation that will convert <code>javax.jms.Message</code>
     * object to <code>org.apache.storm.tuple.Values</code> objects
     * to be emitted.
     *
     * @param producer
     */
    public void setJmsTupleProducer(JmsTupleProducer producer){
        this.tupleProducer = producer;
    }

    /**
     * <code>javax.jms.MessageListener</code> implementation.
     * <p/>
     * Stored the JMS message in an internal queue for processing
     * by the <code>nextTuple()</code> method.
     */
    public void onMessage(Message msg) {
        try {
            LOG.info("Queuing msg [" + msg.getJMSMessageID() + "]");
        } catch (JMSException e) {
        }
        this.queue.offer(msg);
    }

    /**
     * <code>ISpout</code> implementation.
     * <p/>
     * Connects the JMS spout to the configured JMS destination
     * topic/queue.
     *
     */
    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        if(this.jmsProvider == null){
            throw new IllegalStateException("JMS provider has not been set.");
        }
        if(this.tupleProducer == null){
            throw new IllegalStateException("JMS Tuple Producer has not been set.");
        }
        Long topologyTimeout = (Long)conf.get("topology.message.timeout.secs");
        // TODO fine a way to get the default timeout from storm, so we're not hard-coding to 30 seconds (it could change)
        topologyTimeout = topologyTimeout == null ? 30 : topologyTimeout;
        if( (topologyTimeout.longValue() * 1000 )> this.recoveryPeriod){
            LOG.warn("*** WARNING *** : " +
                    "Recovery period ("+ this.recoveryPeriod + " ms.) is less then the configured " +
                    "'topology.message.timeout.secs' of " + topologyTimeout +
                    " secs. This could lead to a message replay flood!");
        }
        this.queue = new LinkedBlockingQueue<Message>();
        this.toCommit = new TreeSet<JmsMessageID>();
        this.pendingMessages = new HashMap<JmsMessageID, Message>();
        this.collector = collector;
        try {
            ConnectionFactory cf = this.jmsProvider.connectionFactory();
            Destination dest = this.jmsProvider.destination();
            this.connection = cf.createConnection();
            this.session = connection.createSession(false,
                    this.jmsAcknowledgeMode);
            MessageConsumer consumer = session.createConsumer(dest);
            consumer.setMessageListener(this);
            this.connection.start();
            if (this.isDurableSubscription() && this.recoveryPeriod > 0){
                this.recoveryTimer = new Timer();
                this.recoveryTimer.scheduleAtFixedRate(new RecoveryTask(), 10, this.recoveryPeriod);
            }

        } catch (Exception e) {
            LOG.warn("Error creating JMS connection.", e);
        }

    }

    public void close() {
        try {
            LOG.info("Closing JMS connection.");
            this.session.close();
            this.connection.close();
        } catch (JMSException e) {
            LOG.warn("Error closing JMS connection.", e);
        }

    }

    public void nextTuple() {
        Message msg = null;
        
		try {
			msg = this.queue.poll(50, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        if (msg != null) {

            LOG.info("Fetching RefData: " + msg);
            // get the tuple from the handler
            try {
                String symbol = null;
                final double[] stockPrice = {0.0};
                if(msg instanceof TextMessage) {
                    TextMessage textMessage = (TextMessage) msg;
                    String payLoad = textMessage.getText();
//                    LOG.info("Payload: {} ", payLoad);

                    JsonParser parser = new JsonParser();
                    JsonObject jsonObject = (JsonObject)parser.parse(payLoad);
                    symbol = jsonObject.get("symbol").getAsString();
                    stockPrice[0] = jsonObject.get("price").getAsDouble();
//                    LOG.info("Stock Price.. {}", stockPrice[0]);
                }
                List<OptionData> optionTobePublished = null;
                if(readAll) {
                    optionTobePublished = readFromUrl(refDataUrl);//optionDataMap.get(symbol);
                }else{
                    //read
                    optionTobePublished = readFromUrl(refDataUrl+"/"+symbol);
                }
                LOG.info("Fetching  ReData Completed");
                // ack if we're not in AUTO_ACKNOWLEDGE mode, or the message requests ACKNOWLEDGE
//                LOG.info("Requested deliveryMode: " + toDeliveryModeString(msg.getJMSDeliveryMode()));
//                LOG.info("Our deliveryMode: " + toDeliveryModeString(this.jmsAcknowledgeMode));"
                LOG.info("Data Emission Started");
                
                count++;
                String  strBatchId = "BATCH_ID_" + count;
                
                if (this.isDurableSubscription()) {
                    //LOG.info("Requesting acks.");
                    JmsMessageID messageId = new JmsMessageID(this.messageSequence++, msg.getJMSMessageID());
                    if(optionTobePublished != null && !optionTobePublished.isEmpty()) {
                        (optionTobePublished).parallelStream().forEach(optionData -> {
                            optionData.setBatchId(strBatchId);
                            optionData.setStockPrice(stockPrice[0]);
                            Values vals = ((OptionJMSTupleProducer) (this.tupleProducer)).toTuple(optionData,
                                    optionData.getOptionName());
                            this.collector.emit(vals);
                        });
                    }

                    // at this point we successfully emitted. Store
                    // the message and message ID so we can do a
                    // JMS acknowledge later
                    this.pendingMessages.put(messageId, msg);
                    this.toCommit.add(messageId);
                } else {
                    if(optionTobePublished != null && !optionTobePublished.isEmpty()) {
                        (optionTobePublished).parallelStream().forEach(optionData -> {
                            optionData.setBatchId(strBatchId);
                            optionData.setStockPrice(stockPrice[0]);
                            Values vals = ((OptionJMSTupleProducer) (this.tupleProducer)).toTuple(optionData,
                                    optionData.getOptionName());
                            this.collector.emit(vals);
                        });
                    }
                }
                LOG.info("Data Emission Completed");
            } catch (JMSException e) {
                LOG.warn("Unable to convert JMS message: " + msg);
            }

        }

    }

    /*
     * Will only be called if we're transactional or not AUTO_ACKNOWLEDGE
     */
    public void ack(Object msgId) {

        Message msg = this.pendingMessages.remove(msgId);
        JmsMessageID oldest = this.toCommit.first();
        if(msgId.equals(oldest)) {
            if (msg != null) {
                try {
//                    LOG.info("Committing...");
                    msg.acknowledge();
//                    LOG.info("JMS Message acked: " + msgId);
                    this.toCommit.remove(msgId);
                } catch (JMSException e) {
                    LOG.warn("Error acknowledging JMS message: " + msgId, e);
                }
            } else {
                LOG.warn("Couldn't acknowledge unknown JMS message ID: " + msgId);
            }
        } else {
            this.toCommit.remove(msgId);
        }

    }

    /*
     * Will only be called if we're transactional or not AUTO_ACKNOWLEDGE
     */
    public void fail(Object msgId) {
        LOG.warn("Message failed: " + msgId);
        this.pendingMessages.clear();
        this.toCommit.clear();
        synchronized(this.recoveryMutex){
            this.hasFailures = true;
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        this.tupleProducer.declareOutputFields(declarer);

    }

    /**
     * Returns <code>true</code> if the spout has received failures
     * from which it has not yet recovered.
     */
    public boolean hasFailures(){
        return this.hasFailures;
    }

    protected void recovered(){
        this.hasFailures = false;
    }

    /**
     * Sets the periodicity of the timer task that
     * checks for failures and recovers the JMS session.
     *
     * @param period
     */
    public void setRecoveryPeriod(long period){
        this.recoveryPeriod = period;
    }

    public boolean isDistributed() {
        return this.distributed;
    }

    /**
     * Sets the "distributed" mode of this spout.
     * <p/>
     * If <code>true</code> multiple instances of this spout <i>may</i> be
     * created across the cluster (depending on the "parallelism_hint" in the topology configuration).
     * <p/>
     * Setting this value to <code>false</code> essentially means this spout will run as a singleton
     * within the cluster ("parallelism_hint" will be ignored).
     * <p/>
     * In general, this should be set to <code>false</code> if the underlying JMS destination is a
     * topic, and <code>true</code> if it is a JMS queue.
     *
     * @param distributed
     */
    public void setDistributed(boolean distributed){
        this.distributed = distributed;
    }


    private static final String toDeliveryModeString(int deliveryMode) {
        switch (deliveryMode) {
            case Session.AUTO_ACKNOWLEDGE:
                return "AUTO_ACKNOWLEDGE";
            case Session.CLIENT_ACKNOWLEDGE:
                return "CLIENT_ACKNOWLEDGE";
            case Session.DUPS_OK_ACKNOWLEDGE:
                return "DUPS_OK_ACKNOWLEDGE";
            default:
                return "UNKNOWN";

        }
    }

    protected Session getSession(){
        return this.session;
    }

    private boolean isDurableSubscription(){
        return (this.jmsAcknowledgeMode != Session.AUTO_ACKNOWLEDGE);
    }


    private class RecoveryTask extends TimerTask {
        private final Logger LOG = LoggerFactory.getLogger(RecoveryTask.class);

        public void run() {
            synchronized (OptionJmsSpout.this.recoveryMutex) {
                if (OptionJmsSpout.this.hasFailures()) {
                    try {
                        LOG.info("Recovering from a message failure.");
                        OptionJmsSpout.this.getSession().recover();
                        OptionJmsSpout.this.recovered();
                    } catch (JMSException e) {
                        LOG.warn("Could not recover jms session.", e);
                    }
                }
            }
        }
    }

    private List<OptionData> readFromUrl(String refDataUrl) {
        HttpURLConnection conn = null;
        URL url = null;
        JsonObject jObj = null;
        List<OptionData> optionDataList = null;

        try {
            url = new URL(refDataUrl);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.connect();

            InputStream in = new BufferedInputStream(conn.getInputStream());
            StringBuilder sb = new StringBuilder();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            JsonParser parser = new JsonParser();
            Object obj = parser.parse(sb.toString());
            JsonArray jarray = (JsonArray)obj;
            optionDataList = new ArrayList<>();
            for(int i=0; i < jarray.size(); i++){
                optionDataList.add(OptionData.gson.fromJson(jarray.get(i).getAsJsonObject(), OptionData.class));
            }
            //OptionData[] optionDataArray = OptionData.gson.fromJson(reader, OptionData[].class);

            ///optionDataList = Arrays.asList(optionDataArray);
        } catch (Exception ex) {
            LOG.error("Exception reading RefData {}", refDataUrl, ex);
        }
        return optionDataList;

    }

}
