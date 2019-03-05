package com.reporting.mocks.publishing.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

public class KafkaConfig {
    @Autowired
    private Environment environment;

    public String getKafkaServer() {
        return environment.getProperty("kafka.server");
    }

    public String getIntradayRiskSetTopic() {
        return environment.getProperty("kafka.topic.intradayriskset");
    }

    public String getIntradayRiskTickTopic() {
        return environment.getProperty("kafka.topic.intradayrisktick");
    }

    public String getCalculationContextTopic() {
        return environment.getProperty("kafka.topic.calccontext");
    }

    public String getIntradayTradeTopic() {
        return environment.getProperty("kafka.topic.intradaytrade");
    }

    public String getMarketEnvTopic() {
        return environment.getProperty("kafka.topic.market");
    }
}
