package com.wf.option.pricing.kafka;

import com.wf.option.pricing.model.OptionData;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * Created by hems on 06/05/19.
 */
public class OptionDataSerializer implements Serializer<OptionData> {
    private String encoding = "UTF8";

    public OptionDataSerializer() {
    }

    public void configure(Map<String, ?> configs, boolean isKey) {
        String propertyName = isKey?"key.serializer.encoding":"value.serializer.encoding";
        Object encodingValue = configs.get(propertyName);
        if(encodingValue == null) {
            encodingValue = configs.get("serializer.encoding");
        }

        if(encodingValue != null && encodingValue instanceof String) {
            this.encoding = (String)encodingValue;
        }

    }

    @Override
    public byte[] serialize(String s, OptionData optionData) {
        try {
            return optionData == null?null: optionData.toJSONString().getBytes(this.encoding);
        } catch (UnsupportedEncodingException var4) {
            throw new SerializationException("Error when serializing OptionData to byte[] due to unsupported encoding " + this.encoding);
        }
    }

    public void close() {
    }
}
