package com.reporting.mocks.publishing.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class KafkaConfig {
    @Autowired
    private Environment environment;

    public String getKafkaServer() {
        return this.environment.getProperty("kafka.server");
    }
}
