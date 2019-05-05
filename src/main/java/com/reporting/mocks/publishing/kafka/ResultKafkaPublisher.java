package com.reporting.mocks.publishing.kafka;

import com.reporting.mocks.interfaces.publishing.IResultPublisher;
import com.reporting.mocks.interfaces.publishing.IResultPublisherConfiguration;
import com.reporting.mocks.model.*;
import com.reporting.mocks.model.risks.Risk;

import org.springframework.stereotype.Component;

@Component
public class ResultKafkaPublisher implements IResultPublisher {
    KafkaConfig appConfig;

    protected IResultPublisherConfiguration resultsPublisherConfiguration;
    protected RiskResultSetKafkaProducer riskResultSetProducer;
    protected RiskResultKafkaProducer riskResultProducer;
    protected CalculationContextKafkaProducer calculationContextProducer;
    protected TradeKafkaPublisher tradeKafkaPublisher;
    protected MarketEnvKafkaPublisher marketEnvKafkaPublisher;

    public ResultKafkaPublisher(IResultPublisherConfiguration iResultPublisherConfiguration) {
        this.appConfig = new KafkaConfig();
        this.riskResultSetProducer = new RiskResultSetKafkaProducer(iResultPublisherConfiguration, appConfig);
        this.riskResultProducer = new RiskResultKafkaProducer(iResultPublisherConfiguration, appConfig);
        this.calculationContextProducer = new CalculationContextKafkaProducer(iResultPublisherConfiguration, appConfig);
        this.tradeKafkaPublisher = new TradeKafkaPublisher(iResultPublisherConfiguration, appConfig);
        this.marketEnvKafkaPublisher = new MarketEnvKafkaPublisher(iResultPublisherConfiguration, appConfig);
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
    public void publishIntradayTrade(TradeLifecycle tradeLifecycle) {
        this.tradeKafkaPublisher.send(tradeLifecycle);
    }

    @Override
    public void publishEndofDayRiskRun(RiskResultSet riskResultSet) {

    }

    @Override
    public void publishIntradayRiskResult(RiskResult<? extends Risk> riskResult) {
        this.riskResultProducer.send(riskResult);
    }
}
