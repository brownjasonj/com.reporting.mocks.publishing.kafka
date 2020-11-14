package com.reporting.mocks.publishing.kafka;

import com.google.gson.Gson;
import com.reporting.mocks.interfaces.publishing.IResultPublisherConfiguration;
import com.reporting.mocks.model.TradeLifecycle;
import com.reporting.mocks.model.trade.Trade;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;
import java.util.logging.Logger;

public class TradeProducer {
    private static final Logger LOGGER = Logger.getLogger( TradeProducer.class.getName() );
    private String BOOTSTRAPSERVER = null;
    private String TOPIC = null;
    private Properties kafkaProperties;
    private Producer<UUID,String> producer = null;

    public TradeProducer(IResultPublisherConfiguration resultsPublisherConfiguration, KafkaConfig appConfig) {
        this.TOPIC = resultsPublisherConfiguration.getIntradayTradeTopic();
        this.BOOTSTRAPSERVER = appConfig.getKafkaServer();

        this.kafkaProperties = new Properties();

        this.kafkaProperties.put("bootstrap.servers", this.BOOTSTRAPSERVER);
        this.kafkaProperties.put("key.serializer", "com.reporting.kafka.serialization.UUIDSerializer");
        this.kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        if (this.TOPIC != null && !this.TOPIC.isEmpty())
            this.producer = new KafkaProducer<UUID,String>(this.kafkaProperties);
    }

    protected void printTradeEvent(TradeLifecycle tradeLifecycle) {
        Trade trade = null;
        switch (tradeLifecycle.getLifecycleType()) {
            case New:
            case Modify:
                trade = tradeLifecycle.getTradeAfterLifeCycle();
                break;
            case Delete:
                trade = tradeLifecycle.getTradeBeforeLifeCycle();
        }
        LOGGER.info("TLE >>> " + tradeLifecycle.getLifecycleType() + " " + trade.getBook() + " " + trade.getTradeType() + " " + trade.getTcn() + " " );
    }

    public void send(TradeLifecycle tradeLifecycle) {
        if (producer != null) {
            Gson gson = new Gson();
            String tradeLifeCycleJson = gson.toJson(tradeLifecycle);
            printTradeEvent(tradeLifecycle);

            // a lifecycle event has two trade states, before and after.  however one maybe null in the case
            // of new and delete.  Since we are using the tcn of a trade as the key for the kafka topic we
            // have to check which state exists.  In this case we prefer to use the afterstate if possible.
            UUID kafkaMessageKey = null;
            if (tradeLifecycle.getTradeAfterLifeCycle() != null) {
                kafkaMessageKey = tradeLifecycle.getTradeAfterLifeCycle().getTcn().getId();
            }
            else if (tradeLifecycle.getTradeBeforeLifeCycle() != null) {
                kafkaMessageKey = tradeLifecycle.getTradeBeforeLifeCycle().getTcn().getId();
            }
            else {
                kafkaMessageKey = UUID.randomUUID();
            }


            ProducerRecord<UUID, String> record = new ProducerRecord<>(this.TOPIC, kafkaMessageKey, tradeLifeCycleJson);
            try {
                this.producer.send(record).get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
