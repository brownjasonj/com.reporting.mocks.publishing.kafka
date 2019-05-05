package com.reporting.mocks.publishing.kafka;

import com.reporting.mocks.model.TradePopulation;
// import org.apache.kafka.clients.producer.KafkaProducer;
// import org.apache.kafka.clients.producer.Producer;
// import org.apache.kafka.clients.producer.ProducerRecord;

// import java.util.Properties;
// import java.util.UUID;

public class TradePopulationKafkaProducer {
//     private final String BOOTSTRAPSERVER =  "localhost:9092";
//    private final String TRADEPOPULATUONTOPIC = "TradePopulation";
//     private Properties kafkaProperties;
//    private Producer<UUID,TradePopulation> producer;

    public TradePopulationKafkaProducer() {
        // this.kafkaProperties = new Properties();

        // this.kafkaProperties.put("bootstrap.servers", this.BOOTSTRAPSERVER);
        // this.kafkaProperties.put("key.serializer", "com.reporting.kafka.serialization.UUIDSerializer");
        // this.kafkaProperties.put("value.serializer", "com.reporting.mocks.endpoints.kafka.RiskRunResult");

        // this.producer = new KafkaProducer<UUID,TradePopulation>(this.kafkaProperties);
    }

    public void sendMessage(TradePopulation tradePopulation) {
    //    ProducerRecord<UUID, TradePopulation> record = new ProducerRecord<>(this.TRADEPOPULATUONTOPIC, tradePopulation.getId(), tradePopulation);
    //    try {
    //        this.producer.send(record);
    //    }
    //    catch (Exception e) {
    //        e.printStackTrace();
    //    }
    }
}
