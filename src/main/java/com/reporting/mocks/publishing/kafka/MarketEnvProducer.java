package com.reporting.mocks.publishing.kafka;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.reporting.mocks.interfaces.publishing.IResultPublisherConfiguration;
import com.reporting.mocks.model.MarketEnv;
import com.reporting.mocks.publishing.kafka.gson.InstantSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;

public class MarketEnvProducer {
    private static final Logger LOGGER = Logger.getLogger( MarketEnvProducer.class.getName() );
    private String BOOTSTRAPSERVER;
    private String TOPIC;
    private Properties kafkaProperties;
    private Producer<UUID,String> producer;
    private GsonBuilder gsonBuilder;

    public MarketEnvProducer(IResultPublisherConfiguration resultsPublisherConfiguration, KafkaConfig appConfig) {
        this.TOPIC = resultsPublisherConfiguration.getMarketEnvTopic();
        this.BOOTSTRAPSERVER = appConfig.getKafkaServer();

        this.kafkaProperties = new Properties();

        this.kafkaProperties.put("bootstrap.servers", this.BOOTSTRAPSERVER);
        this.kafkaProperties.put("key.serializer", "com.reporting.mocks.publishing.kafka.serialization.UUIDSerializer");
        this.kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        if (this.TOPIC != null && !this.TOPIC.isEmpty())
            this.producer = new KafkaProducer<UUID,String>(this.kafkaProperties);
        this.gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(Instant.class, new InstantSerializer());

    }

    public void send(MarketEnv marketEnv) {
        if (this.producer != null) {
            Gson gson = this.gsonBuilder.setPrettyPrinting().create();

            LOGGER.info("MKT >>> " + marketEnv.getId() + " " + marketEnv.getType() + " " + marketEnv.getPricingGroup().getName());

            ProducerRecord<UUID, String> record = new ProducerRecord<>(this.TOPIC, marketEnv.getId().getId(), gson.toJson(marketEnv));
            try {
                this.producer.send(record).get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
