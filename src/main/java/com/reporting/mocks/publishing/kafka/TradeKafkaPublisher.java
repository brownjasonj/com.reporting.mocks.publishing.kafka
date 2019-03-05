package com.reporting.mocks.publishing.kafka;

import com.google.gson.Gson;
import com.reporting.mocks.model.TradeLifecycle;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;

public class TradeKafkaPublisher {
    private String BOOTSTRAPSERVER = null;
    private String TOPIC = null;
    private Properties kafkaProperties;
    private Producer producer = null;

    public TradeKafkaPublisher(KafkaConfig appConfig) {
        this.TOPIC = appConfig.getIntradayTradeTopic();
        this.BOOTSTRAPSERVER = appConfig.getKafkaServer();

        this.kafkaProperties = new Properties();

        this.kafkaProperties.put("bootstrap.servers", this.BOOTSTRAPSERVER);
        this.kafkaProperties.put("key.serializer", "com.reporting.kafka.serialization.UUIDSerializer");
        this.kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        if (this.TOPIC != null && !this.TOPIC.isEmpty())
            this.producer = new KafkaProducer<UUID,String>(this.kafkaProperties);
    }

    public void send(TradeLifecycle tradeLifecycle) {
        if (producer != null) {
            Gson gson = new Gson();
            String tradeLifeCycleJson = gson.toJson(tradeLifecycle);
            System.out.println("TradeLifecycle: " + tradeLifeCycleJson);
            ProducerRecord<UUID, String> record = new ProducerRecord<>(this.TOPIC, tradeLifecycle.getTrade().getTcn().getId(), tradeLifeCycleJson);
            try {
                this.producer.send(record).get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
