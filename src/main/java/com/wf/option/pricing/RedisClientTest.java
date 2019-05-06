package com.wf.option.pricing;

import redis.clients.jedis.Jedis;

/**
 * Created by hems on 07/05/19.
 */
public class RedisClientTest {
    public static void main(String[] args) {
        //Connecting to Redis server on localhost
        Jedis jedis = new Jedis("optpricing.3monrn.0001.use2.cache.amazonaws.com",6379);
        System.out.println("Connection to server sucessfully");
        //set the data in redis string
        jedis.set("tutorial-name", "Redis tutorial");
        // Get the stored data and print it
        System.out.println("Stored string in redis:: "+ jedis.get("tutorial-name"));
        System.out.println(jedis.get("tutorial-name"));
        jedis.close();
    }
}
