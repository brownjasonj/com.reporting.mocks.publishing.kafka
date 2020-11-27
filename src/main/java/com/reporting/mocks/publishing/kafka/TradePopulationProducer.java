package com.reporting.mocks.publishing.kafka;

import com.reporting.mocks.interfaces.persistence.ITradePopulation;
// import org.apache.kafka.clients.producer.KafkaProducer;
// import org.apache.kafka.clients.producer.Producer;
// import org.apache.kafka.clients.producer.ProducerRecord;

// import java.util.Properties;
// import java.util.UUID;

public class TradePopulationProducer {
//     private final String BOOTSTRAPSERVER =  "localhost:9092";
//    private final String TRADEPOPULATUONTOPIC = "ITradePopulation";
//     private Properties kafkaProperties;
//    private Producer<UUID,ITradePopulation> producer;

    public TradePopulationProducer() {
        // this.kafkaProperties = new Properties();

        // this.kafkaProperties.put("bootstrap.servers", this.BOOTSTRAPSERVER);
        // this.kafkaProperties.put("key.serializer", "com.reporting.kafka.serialization.UUIDSerializer");
        // this.kafkaProperties.put("value.serializer", "com.reporting.mocks.endpoints.kafka.RiskRunResult");

        // this.producer = new KafkaProducer<UUID,ITradePopulation>(this.kafkaProperties);
    }

    public void sendMessage(ITradePopulation tradePopulation) {
    //    ProducerRecord<UUID, ITradePopulation> record = new ProducerRecord<>(this.TRADEPOPULATUONTOPIC, tradePopulation.getId(), tradePopulation);
    //    try {
    //        this.producer.send(record);
    //    }
    //    catch (Exception e) {
    //        e.printStackTrace();
    //    }
    }
}
