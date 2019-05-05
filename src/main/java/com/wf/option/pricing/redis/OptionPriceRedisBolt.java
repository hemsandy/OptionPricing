package com.wf.option.pricing.redis;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by hems on 05/05/19.
 */
public class OptionPriceRedisBolt extends AbstractRedisBolt {

    private Logger logger = LoggerFactory.getLogger(OptionPriceRedisBolt.class);

    private final RedisStoreMapper storeMapper;
    private final RedisDataTypeDescription.RedisDataType dataType;
    private final String additionalKey;

    private JedisPool jedisPool;
    private String host;private int port;
    private long counter = 0;

    protected LinkedBlockingQueue<Tuple> tupleQueue = new LinkedBlockingQueue<>();

    /**
     * Constructor for single Redis environment (JedisPool)
     * @param config configuration for initializing JedisPool
     * @param storeMapper mapper containing which datatype, storing value's key that Bolt uses
     */
    public OptionPriceRedisBolt(JedisPoolConfig config, RedisStoreMapper storeMapper) {
        super(config);
        this.storeMapper = storeMapper;
        RedisDataTypeDescription dataTypeDescription = storeMapper.getDataTypeDescription();
        this.dataType = dataTypeDescription.getDataType();
        this.additionalKey = dataTypeDescription.getAdditionalKey();
        this.host = config.getHost();
        this.port = config.getPort();
    }


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        logger.info("prepare-JEDIS POOL");
        super.prepare(map, topologyContext, collector);
        this.jedisPool = new JedisPool(host, port);
    }

    /**
     * Constructor for Redis Cluster environment (JedisCluster)
     * @param config configuration for initializing JedisCluster
     * @param storeMapper mapper containing which datatype, storing value's key that Bolt uses
     */
    public OptionPriceRedisBolt(JedisClusterConfig config, RedisStoreMapper storeMapper) {
        super(config);
        this.storeMapper = storeMapper;

        RedisDataTypeDescription dataTypeDescription = storeMapper.getDataTypeDescription();
        this.dataType = dataTypeDescription.getDataType();
        this.additionalKey = dataTypeDescription.getAdditionalKey();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void process(Tuple input) {

        try {
            if(isTickTuple(input)) {
                //Write to Redis
                logger.info("TICK_TUPLE. RECEIVED");
                writeToRedis();
            }else {
                tupleQueue.add(input);
                counter ++;
                if(counter >= 1000) {
                    writeToRedis();
                    counter = 0;
                }
            }

            collector.ack(input);
        } catch (Exception e) {
            this.collector.reportError(e);
            this.collector.fail(input);
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        logger.info("getComponentConfiguration..");
        // configure how often a tick tuple will be sent to our bolt
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
        return conf;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    protected static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }


    private int writeToRedis() throws Exception{
        logger.info("writeToRedis..start");
        Jedis jedis = null;
        int status = -1;
        try {
            jedis = jedisPool.getResource();
            Tuple eachTuple = null;
            Pipeline pipeline = jedis.pipelined();
            while ((eachTuple=tupleQueue.poll()) != null) {
                String key = storeMapper.getKeyFromTuple(eachTuple);
                String value = storeMapper.getValueFromTuple(eachTuple);
                pipeline.set(key,value);
            }

            status = 0;
        }catch(Exception ex) {
            throw ex;
        }finally {
            returnInstance(jedis);
        }
        logger.info("writeToRedis..end");
        return status;
    }

    @Override
    public void cleanup() {
        try {
            logger.info("Writing in cleanup..");
            writeToRedis();

        } catch (Exception e) {
            e.printStackTrace();
        }
        super.cleanup();
    }
}