package com.reporting.mocks.publishing.kafka;

import com.reporting.mocks.interfaces.publishing.IResultPublisher;
import com.reporting.mocks.model.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
public class ResultKafkaPublisher implements IResultPublisher {
    KafkaConfig appConfig;

    protected RiskResultSetKafkaProducer riskResultSetProducer;
    protected RiskResultKafkaProducer riskResultProducer;
    protected CalculationContextKafkaProducer calculationContextProducer;
    protected TradeKafkaPublisher tradeKafkaPublisher;
    protected MarketEnvKafkaPublisher marketEnvKafkaPublisher;

    public ResultKafkaPublisher() {
        this.appConfig = new KafkaConfig();
        this.riskResultSetProducer = new RiskResultSetKafkaProducer(appConfig);
        this.riskResultProducer = new RiskResultKafkaProducer(appConfig);
        this.calculationContextProducer = new CalculationContextKafkaProducer(appConfig);
        this.tradeKafkaPublisher = new TradeKafkaPublisher(appConfig);
        this.marketEnvKafkaPublisher = new MarketEnvKafkaPublisher(appConfig);
    }

    @Override
    public void publish(CalculationContext calculationContext) {
        this.calculationContextProducer.send(calculationContext);
    }

    @Override
    public void publish(MarketEnv marketEnv) {
        this.marketEnvKafkaPublisher.send(marketEnv);
    }

    @Override
    public void publishIntradayRiskResultSet(RiskResultSet riskResultSet) {
        this.riskResultSetProducer.send(riskResultSet);
    }


    @Override
    public void publishIntradayRiskResult(RiskResult riskResult) { this.riskResultProducer.send(riskResult); }

    @Override
    public void publishIntradayTrade(TradeLifecycle tradeLifecycle) {
        this.tradeKafkaPublisher.send(tradeLifecycle);
    }

    @Override
    public void publishEndofDayRiskRun(RiskResultSet riskResultSet) {

    }
}
