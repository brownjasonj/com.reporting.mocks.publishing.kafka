package com.reporting.mocks.publishing.kafka;

import com.google.gson.Gson;
import com.reporting.mocks.interfaces.publishing.IResultPublisherConfiguration;
import com.reporting.mocks.model.MarketEnv;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;

public class MarketEnvKafkaPublisher {
    private String BOOTSTRAPSERVER;
    private String TOPIC;
    private Properties kafkaProperties;
    private Producer<UUID,String> producer;

    public MarketEnvKafkaPublisher(IResultPublisherConfiguration resultsPublisherConfiguration, KafkaConfig appConfig) {
        this.TOPIC = resultsPublisherConfiguration.getMarketEnvTopic();
        this.BOOTSTRAPSERVER = appConfig.getKafkaServer();

        this.kafkaProperties = new Properties();

        this.kafkaProperties.put("bootstrap.servers", this.BOOTSTRAPSERVER);
        this.kafkaProperties.put("key.serializer", "com.reporting.kafka.serialization.UUIDSerializer");
        this.kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        if (this.TOPIC != null && !this.TOPIC.isEmpty())
            this.producer = new KafkaProducer<UUID,String>(this.kafkaProperties);
    }

    public void send(MarketEnv marketEnv) {
        if (this.producer != null) {
            Gson gson = new Gson();
            ProducerRecord<UUID, String> record = new ProducerRecord<>(this.TOPIC, marketEnv.getId().getId(), gson.toJson(marketEnv));
            try {
                this.producer.send(record).get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
