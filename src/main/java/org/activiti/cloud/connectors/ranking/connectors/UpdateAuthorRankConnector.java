package org.activiti.cloud.connectors.ranking.connectors;

import java.util.HashMap;
import java.util.Map;

import org.activiti.cloud.api.process.model.IntegrationRequest;
import org.activiti.cloud.api.process.model.IntegrationResult;
import org.activiti.cloud.connectors.ranking.services.RankingService;
import org.activiti.cloud.connectors.starter.channels.IntegrationResultSender;
import org.activiti.cloud.connectors.starter.configuration.ConnectorProperties;
import org.activiti.cloud.connectors.starter.model.IntegrationResultBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import static net.logstash.logback.marker.Markers.append;

@Component
@EnableBinding(RankingConnectorChannels.class)
public class UpdateAuthorRankConnector {

    private final IntegrationResultSender integrationResultSender;
    private final RankingService rankingService;
    private Logger logger = LoggerFactory.getLogger(UpdateAuthorRankConnector.class);
    @Value("${spring.application.name}")
    private String appName;
    @Autowired
    private ConnectorProperties connectorProperties;

    public UpdateAuthorRankConnector(IntegrationResultSender integrationResultSender,
                                     RankingService rankingService) {
        this.integrationResultSender = integrationResultSender;
        this.rankingService = rankingService;
    }

    @StreamListener(value = RankingConnectorChannels.UPDATE_RANK_CONSUMER)
    public void processEnglish(IntegrationRequest event) throws InterruptedException {

        String author = String.valueOf(event.getIntegrationContext().getInBoundVariables().get("author"));
        String campaign = String.valueOf(event.getIntegrationContext().getInBoundVariables().get("campaign"));
        String attitude = String.valueOf(event.getIntegrationContext().getInBoundVariables().get("attitude"));
        String processedMessage = String.valueOf(event.getIntegrationContext().getInBoundVariables().get("text"));

        logger.info(append("service-name",
                appName),
                ">>> Rank author: " + author + " related to the campaign: " + campaign + " with attitude/sentiment score: " + attitude + " - > " + processedMessage);

        rankingService.rank(campaign + "-" + attitude,
                author);

        Map<String, Object> results = new HashMap<>();

        Message<IntegrationResult> message = IntegrationResultBuilder.resultFor(event, connectorProperties)
                .withOutboundVariables(results)
                .buildMessage();
        integrationResultSender.send(message);
    }

    @StreamListener(value = RankingConnectorChannels.GET_RANK_CONSUMER)
    public void getRanks(IntegrationRequest event) throws InterruptedException {

        String campaign = String.valueOf(event.getIntegrationContext().getInBoundVariables().get("campaign"));
        int top = Integer.valueOf(event.getIntegrationContext().getInBoundVariables().get("nroTopAuthors").toString());

        Map<String, Object> topAuthorsInCampaign = extractTopAuthorsFromCampaign(campaign + "-positive",
                top);

        Message<IntegrationResult> message = IntegrationResultBuilder.resultFor(event, connectorProperties)
                .withOutboundVariables(topAuthorsInCampaign)
                .buildMessage();
        integrationResultSender.send(message);
    }

    private Map<String, Object> extractTopAuthorsFromCampaign(String campaign,
                                                              int top) {
        Map<String, Object> results = new HashMap<>();
        results.put("top",
                rankingService.getTop(campaign,
                        top));
        return results;
    }
}