package com.wf.option.pricing.redis;

import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.springframework.context.ApplicationContext;

/**
 * Created by hems on 03/05/19.
 */
public class RedisBoltBuilder {

    public static RedisStoreBolt createInstance(ApplicationContext context) {

        String redisHost = context.getEnvironment().getProperty("option.pricing.redis.host");
        int redisPort = Integer.parseInt(context.getEnvironment().getProperty("option.pricing.redis.port"));
        int redisTimeout = Integer.parseInt(context.getEnvironment().getProperty("option.pricing.redis.timeout"));
        String redisPwd = context.getEnvironment().getProperty("option.pricing.redis.pswd");
        int redisDB = Integer.parseInt(context.getEnvironment().getProperty("option.pricing.redis.database"));

        JedisPoolConfig jedisPoolConfig =  new JedisPoolConfig.Builder().setHost(redisHost)
                .setPort(redisPort)
                .setTimeout(redisTimeout)
                .build();
        RedisOptionDataMapper redisStoreMapper = new RedisOptionDataMapper();

        RedisStoreBolt storeBolt = new RedisStoreBolt(jedisPoolConfig, redisStoreMapper);

        return storeBolt;

    }
}
