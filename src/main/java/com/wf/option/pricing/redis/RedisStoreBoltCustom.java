package com.wf.option.pricing.redis;

/**
 * Created by hems on 07/05/19.
 */
import org.apache.storm.Config;
import org.apache.storm.redis.bolt.AbstractRedisBolt;
import org.apache.storm.redis.common.config.JedisClusterConfig;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Basic bolt for writing to Redis
 * <p/>
 * Various data types are supported: STRING, LIST, HASH, SET, SORTED_SET, HYPER_LOG_LOG, GEO
 */
public class RedisStoreBoltCustom extends AbstractRedisBolt {
    private final RedisStoreMapper storeMapper;
    private final RedisDataTypeDescription.RedisDataType dataType;
    private final String additionalKey;
    private Logger logger = LoggerFactory.getLogger(RedisStoreBoltCustom.class);
    /**
     * Constructor for single Redis environment (JedisPool)
     * @param config configuration for initializing JedisPool
     * @param storeMapper mapper containing which datatype, storing value's key that Bolt uses
     */
    public RedisStoreBoltCustom(JedisPoolConfig config, RedisStoreMapper storeMapper) {
        super(config);
        this.storeMapper = storeMapper;

        RedisDataTypeDescription dataTypeDescription = storeMapper.getDataTypeDescription();
        this.dataType = dataTypeDescription.getDataType();
        this.additionalKey = dataTypeDescription.getAdditionalKey();
    }

    /**
     * Constructor for Redis Cluster environment (JedisCluster)
     * @param config configuration for initializing JedisCluster
     * @param storeMapper mapper containing which datatype, storing value's key that Bolt uses
     */
    public RedisStoreBoltCustom(JedisClusterConfig config, RedisStoreMapper storeMapper) {
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

            blockingDeque.add(input);

            collector.ack(input);
        } catch (Exception e) {
            this.collector.reportError(e);
            this.collector.fail(input);
        }
    }
    LinkedBlockingDeque<Tuple> blockingDeque = new LinkedBlockingDeque<>();

    
    protected void storeToRedis(RedisDataTypeDescription.RedisDataType datatype,
                                JedisCommands jedisCommand, String key, String value) {
        try {

            switch (dataType) {
                case STRING:
                    jedisCommand.set(key, value);
                    break;

                case LIST:
                    jedisCommand.rpush(key, value);
                    break;

                case HASH:
                    jedisCommand.hset(additionalKey, key, value);
                    break;

                case SET:
                    jedisCommand.sadd(key, value);
                    break;

                case SORTED_SET:
                    jedisCommand.zadd(additionalKey, Double.valueOf(value), key);
                    break;

                case HYPER_LOG_LOG:
                    jedisCommand.pfadd(key, value);
                    break;

                case GEO:
                    String[] array = value.split(":");
                    if (array.length != 2) {
                        throw new IllegalArgumentException("value structure should be longitude:latitude");
                    }

                    double longitude = Double.valueOf(array[0]);
                    double latitude = Double.valueOf(array[1]);
                    jedisCommand.geoadd(additionalKey, longitude, latitude, key);
                    break;

                default:
                    throw new IllegalArgumentException("Cannot process such data type: " + dataType);
            }

        } catch (Exception e) {
            this.collector.reportError(e);
        }
    }

    @Override
    protected void onTickTuple(Tuple tuple) {
        logger.info("RedisStoreBoltCustom..onTickTuple..");

        final JedisCommands[] jedisCommand = {};
        try {
            jedisCommand[0] = getInstance();
            Tuple input = null;
            while((input = blockingDeque.poll())!= null){
                String key = storeMapper.getKeyFromTuple(input);
                String value = storeMapper.getValueFromTuple(input);
                storeToRedis(RedisDataTypeDescription.RedisDataType.STRING,
                        jedisCommand[0], key, value);
            }
        } finally {
            returnInstance(jedisCommand[0]);
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        logger.info("RedisStoreBoltCustom..getComponentConfiguration..");
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
}
