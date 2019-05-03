package com.wf.option.pricing.redis;

import com.wf.option.pricing.model.OptionData;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.tuple.ITuple;

/**
 * Created by hems on 03/05/19.
 */
public class RedisOptionDataMapper implements RedisStoreMapper {
    private RedisDataTypeDescription description;
    private final String hashKey = "wordCount";

    public RedisOptionDataMapper() {
        description = new RedisDataTypeDescription(
                RedisDataTypeDescription.RedisDataType.HASH, hashKey);
    }

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return description;
    }

    @Override
    public String getKeyFromTuple(ITuple tuple) {
        return ((OptionData) tuple.getValueByField("optionDataWithPrice")).getOptionName();
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
        return ((OptionData) tuple.getValueByField("optionDataWithPrice")).toJSONString();
    }
}

