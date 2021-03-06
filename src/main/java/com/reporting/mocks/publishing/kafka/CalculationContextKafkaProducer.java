package com.reporting.mocks.publishing.kafka;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.reporting.mocks.interfaces.publishing.IResultPublisherConfiguration;
import com.reporting.mocks.model.CalculationContext;
import com.reporting.mocks.publishing.kafka.gson.InstantSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import scala.reflect.internal.pickling.UnPickler;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;


public class CalculationContextKafkaProducer {
    private static final Logger LOGGER = Logger.getLogger( CalculationContextKafkaProducer.class.getName() );
    private String BOOTSTRAPSERVER = null;
    private String TOPIC = null;
    private Properties kafkaProperties = null;
    private Producer<UUID,String> producer = null;
    private GsonBuilder gsonBuilder;

    public CalculationContextKafkaProducer(IResultPublisherConfiguration resultsPublisherConfiguration, KafkaConfig appConfig) {
        this.TOPIC = resultsPublisherConfiguration.getCalculationContextTopic();
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

    public void send(CalculationContext calculationContext) {
        if (this.producer != null) {
            Gson gson = this.gsonBuilder.setPrettyPrinting().create();

            LOGGER.info("CCC >>> " + calculationContext.getPricingGroup().getName() + " " + calculationContext.getMarkets());

            ProducerRecord<UUID, String> record = new ProducerRecord<>(this.TOPIC, calculationContext.getCalculationContextId().getId(), gson.toJson(calculationContext));
            try {
                this.producer.send(record).get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
