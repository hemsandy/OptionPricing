package com.wf.option.pricing.redis;

import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;

import java.util.Map;

/**
 * Created by hems on 03/05/19.
 */
public class RedisBoltBuilder {

    Map<String, String> configuration;
    public RedisBoltBuilder(Map<String, String> configuration){
        this.configuration = configuration;
    }

    public OptionPriceRedisBolt createInstance() {

        String redisHost = configuration.get("redisHost");
        int redisPort = Integer.parseInt(configuration.get("redisPort"));
        int redisTimeout = Integer.parseInt(configuration.get("redisTimeout"));
        String redisPwd = configuration.get("redisPswd");
        int redisDB = Integer.parseInt(configuration.get("redisDatabase"));

        JedisPoolConfig jedisPoolConfig =  new JedisPoolConfig.Builder().setHost(redisHost)
                .setPort(redisPort)
                .setTimeout(redisTimeout)
                .setDatabase(0)
                .build();
        RedisOptionDataMapper redisStoreMapper = new RedisOptionDataMapper();

        OptionPriceRedisBolt storeBolt = new OptionPriceRedisBolt(jedisPoolConfig, redisStoreMapper);

        return storeBolt;

    }
}
