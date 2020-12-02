package com.reporting.mocks.publishing.kafka;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.reporting.mocks.interfaces.publishing.IResultPublisherConfiguration;
import com.reporting.mocks.model.RiskResult;
import com.reporting.mocks.model.risks.Risk;

import com.reporting.mocks.publishing.kafka.gson.InstantSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.ConsoleHandler;

public class IntradayRiskResultProducer {
    private static final Logger LOGGER = Logger.getLogger( IntradayRiskResultProducer.class.getName() );
    private String BOOTSTRAPSERVER = null;
    private String TOPIC = null;
    private Properties kafkaProperties = null;
    private Producer<UUID, String> producer = null;
    private GsonBuilder gsonBuilder;

    public IntradayRiskResultProducer(IResultPublisherConfiguration resultsPublisherConfiguration, KafkaConfig appConfig) {
        this.TOPIC = resultsPublisherConfiguration.getIntradayRiskTickTopic();
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

    public void send(RiskResult<? extends Risk> riskResult) {
        if (this.producer != null) {
            Gson gson = this.gsonBuilder.setPrettyPrinting().create();
            String riskResultJson = gson.toJson(riskResult);
            LOGGER.fine("IRR >>> " + riskResult.getRiskRunId() + " [" + riskResult.getFragmentNo() + "/" + riskResult.getFragmentCount() + "] " + riskResult.getMarketEnvId() + " " + riskResult.getRisk().getRiskType() + " " + riskResult.getRisk().getBookName() + " " + riskResult.getRisk().getTcn().toString());
            LOGGER.finest(riskResultJson);
            // System.out.println("IRR >>> " + riskResult.getRiskRunId() + " [" + riskResult.getFragmentNo() + "/" + riskResult.getFragmentCount() + "] " + riskResult.getMarketEnvId() + " " + riskResult.getRisk().getRiskType() + " " + riskResult.getRisk().getBookName() + " " + riskResult.getRisk().getTcn().toString());
            ProducerRecord<UUID, String> record = new ProducerRecord<>(this.TOPIC, riskResult.getRiskRunId().getId(), riskResultJson);
            try {
                this.producer.send(record).get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
