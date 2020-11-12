package com.reporting.mocks.publishing.kafka;

import com.reporting.mocks.interfaces.publishing.IResultPublisher;
import com.reporting.mocks.interfaces.publishing.IResultPublisherConfiguration;
import com.reporting.mocks.model.*;
import com.reporting.mocks.model.risks.Risk;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ResultKafkaPublisher implements IResultPublisher {
    protected IntradayRiskResultSetProducer riskResultSetProducer;
    protected IntradayRiskResultProducer riskResultProducer;
    protected EndOfDayRiskResultProducer endOfDayRiskResultProducer;
    protected EndOfDayRiskResultSetProducer endOfDayRiskResultSetProducer;
    protected StartOfDayRiskResultProducer startOfDayRiskResultProducer;
    protected StartOfDayRiskResultSetProducer startOfDayRiskResultSetProducer;
    protected CalculationContextKafkaProducer calculationContextProducer;
    protected TradeProducer tradeKafkaPublisher;
    protected MarketEnvProducer marketEnvKafkaPublisher;

    @Autowired
    public ResultKafkaPublisher(IResultPublisherConfiguration iResultPublisherConfiguration, KafkaConfig appConfig) {
        this.riskResultSetProducer = new IntradayRiskResultSetProducer(iResultPublisherConfiguration, appConfig);
        this.riskResultProducer = new IntradayRiskResultProducer(iResultPublisherConfiguration, appConfig);
        this.endOfDayRiskResultProducer = new EndOfDayRiskResultProducer(iResultPublisherConfiguration, appConfig);
        this.endOfDayRiskResultSetProducer = new EndOfDayRiskResultSetProducer(iResultPublisherConfiguration, appConfig);
        this.startOfDayRiskResultProducer = new StartOfDayRiskResultProducer(iResultPublisherConfiguration, appConfig);
        this.startOfDayRiskResultSetProducer = new StartOfDayRiskResultSetProducer(iResultPublisherConfiguration, appConfig);
        this.calculationContextProducer = new CalculationContextKafkaProducer(iResultPublisherConfiguration, appConfig);
        this.tradeKafkaPublisher = new TradeProducer(iResultPublisherConfiguration, appConfig);
        this.marketEnvKafkaPublisher = new MarketEnvProducer(iResultPublisherConfiguration, appConfig);
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
    public void publishIntradayRiskResult(RiskResult<? extends Risk> riskResult) {
        this.riskResultProducer.send(riskResult);
    }

    @Override
    public void publishEndOfDayRiskResultSet(RiskResultSet riskResultSet) {
        this.endOfDayRiskResultSetProducer.send(riskResultSet);
    }

    @Override
    public void publishEndOfDayRiskResult(RiskResult<? extends Risk> riskResult) {
        this.endOfDayRiskResultProducer.send(riskResult);
    }

    @Override
    public void publishStartOfDayRiskResultSet(RiskResultSet riskResultSet) {
        this.startOfDayRiskResultSetProducer.send(riskResultSet);
    }

    @Override
    public void publishStartOfDayRiskResult(RiskResult<? extends Risk> riskResult) {
        this.startOfDayRiskResultProducer.send(riskResult);
    }
}
