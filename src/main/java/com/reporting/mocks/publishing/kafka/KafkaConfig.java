package com.reporting.mocks.publishing.kafka;


public class KafkaConfig {

    public String getKafkaServer() {
        return "localhost:9092";
    }

    public String getIntradayRiskSetTopic() {
        return "IntraDayRiskSet";
    }

    public String getIntradayRiskTickTopic() {
        return "IntraDayRiskTick";
    }

    public String getCalculationContextTopic() {
        return "CalculationContext";
    }

    public String getIntradayTradeTopic() {
        return "IntraDayTrade";
    }

    public String getMarketEnvTopic() {
        return "MarketEnv";
    }
}
