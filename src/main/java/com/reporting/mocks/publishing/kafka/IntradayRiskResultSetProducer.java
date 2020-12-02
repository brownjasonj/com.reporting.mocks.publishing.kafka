package com.reporting.mocks.publishing.kafka;

import com.google.gson.Gson;
import com.reporting.mocks.interfaces.publishing.IResultPublisherConfiguration;
import com.reporting.mocks.model.RiskResultSet;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;

public class IntradayRiskResultSetProducer {
    private String BOOTSTRAPSERVER = null;
    private String TOPIC = null;
    private Properties kafkaProperties = null;
    private Producer<UUID, String> producer = null;

    public IntradayRiskResultSetProducer(IResultPublisherConfiguration resultsPublisherConfiguration, KafkaConfig appConfig) {
        this.TOPIC = resultsPublisherConfiguration.getIntradayRiskSetTopic();
        this.BOOTSTRAPSERVER = appConfig.getKafkaServer();

        this.kafkaProperties = new Properties();

        this.kafkaProperties.put("bootstrap.servers", this.BOOTSTRAPSERVER);
        this.kafkaProperties.put("key.serializer", "com.reporting.mocks.publishing.kafka.serialization.UUIDSerializer");
        this.kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        if (this.TOPIC != null && !this.TOPIC.isEmpty())
            this.producer = new KafkaProducer<UUID,String>(this.kafkaProperties);
    }

    public void send(RiskResultSet riskResultSet) {
        if (this.producer != null) {
            Gson gson = new Gson();
            String riskResultSetJson = gson.toJson(riskResultSet);
            // System.out.println("RiskResultSet: " + riskResultSet);
            ProducerRecord<UUID, String> record = new ProducerRecord<>(this.TOPIC, riskResultSet.getRiskRunId().getId(), riskResultSetJson);
            try {
                this.producer.send(record).get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
