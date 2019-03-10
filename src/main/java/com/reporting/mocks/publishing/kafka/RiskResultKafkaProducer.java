package com.reporting.mocks.publishing.kafka;

import com.google.gson.Gson;
import com.reporting.mocks.interfaces.publishing.IResultPublisherConfiguration;
import com.reporting.mocks.model.RiskResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;

public class RiskResultKafkaProducer {
    private String BOOTSTRAPSERVER = null;
    private String TOPIC = null;
    private Properties kafkaProperties = null;
    private Producer producer = null;

    public RiskResultKafkaProducer(IResultPublisherConfiguration resultsPublisherConfiguration, KafkaConfig appConfig) {
        this.TOPIC = resultsPublisherConfiguration.getIntradayRiskTickTopic();
        this.BOOTSTRAPSERVER = appConfig.getKafkaServer();

        this.kafkaProperties = new Properties();

        this.kafkaProperties.put("bootstrap.servers", this.BOOTSTRAPSERVER);
        this.kafkaProperties.put("key.serializer", "com.reporting.kafka.serialization.UUIDSerializer");
        this.kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        if (this.TOPIC != null && !this.TOPIC.isEmpty())
            this.producer = new KafkaProducer<UUID,String>(this.kafkaProperties);
    }

    public void send(RiskResult riskResult) {
        if (this.producer != null) {
            Gson gson = new Gson();
            String riskResultJson = gson.toJson(riskResult);
            // System.out.println("RiskResult: " + riskResultJson);
            ProducerRecord<UUID, String> record = new ProducerRecord<>(this.TOPIC, riskResult.getRiskRunId().getId(), riskResultJson);
            try {
                this.producer.send(record).get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
