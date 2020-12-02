package com.reporting.mocks.publishing.kafka.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import java.util.UUID;

public class UUIDDeserializer implements Deserializer<UUID> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public UUID deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();
        UUID uuid = null;
        try {
            uuid = mapper.readValue(data, UUID.class);
        } catch (Exception e) {

            e.printStackTrace();
        }
        return uuid;
    }

    @Override
    public void close() {

    }
}
