package org.camunda.tngp.broker.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.camunda.tngp.logstreams.log.LogStream.DEFAULT_TOPIC_NAME;

import java.util.List;
import java.util.stream.Collectors;

import org.camunda.tngp.broker.test.EmbeddedBrokerRule;
import org.camunda.tngp.test.broker.protocol.clientapi.ClientApiRule;
import org.camunda.tngp.test.broker.protocol.clientapi.ExecuteCommandResponse;
import org.camunda.tngp.test.util.TestUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

public class TopicSubscriptionThrottlingTest
{
    protected static final String SUBSCRIPTION_NAME = "foo";

    public EmbeddedBrokerRule brokerRule = new EmbeddedBrokerRule();
    public ClientApiRule apiRule = new ClientApiRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule(brokerRule).around(apiRule);

    protected long subscriberKey;

    public void openSubscription(int prefetchCapacity)
    {
        final ExecuteCommandResponse response = apiRule.createCmdRequest()
            .topicName(DEFAULT_TOPIC_NAME)
            .partitionId(0)
            .eventTypeSubscriber()
            .command()
                .put("startPosition", 0)
                .put("name", "foo")
                .put("eventType", "SUBSCRIBE")
                .put("prefetchCapacity", prefetchCapacity)
                .done()
            .sendAndAwait();
        subscriberKey = response.key();
    }

    @Test
    public void shouldNotPushMoreThanPrefetchCapacity() throws InterruptedException
    {
        // given
        final int nrOfTasks = 5;
        final int prefetchCapacity = 3;

        createTasks(nrOfTasks);

        // when
        openSubscription(prefetchCapacity);

        // then
        TestUtil.waitUntil(() -> apiRule.numSubscribedEventsAvailable() >= 3);
        Thread.sleep(1000L); // there might be more received in case this feature is broken
        assertThat(apiRule.numSubscribedEventsAvailable()).isEqualTo(prefetchCapacity);

    }

    @Test
    public void shouldPushMoreAfterAck() throws InterruptedException
    {
        // given
        final int nrOfTasks = 5;
        final int prefetchCapacity = 3;

        createTasks(nrOfTasks);
        openSubscription(prefetchCapacity);
        TestUtil.waitUntil(() -> apiRule.numSubscribedEventsAvailable() == 3);

        final List<Long> eventPositions = apiRule.subscribedEvents()
                .limit(3)
                .map((e) -> e.position())
                .collect(Collectors.toList());

        apiRule.moveMessageStreamToTail();

        // when
        apiRule.createCmdRequest()
            .topicName(DEFAULT_TOPIC_NAME)
            .partitionId(0)
            .eventTypeSubscription()
            .command()
                .put("name", SUBSCRIPTION_NAME)
                .put("eventType", "ACKNOWLEDGE")
                .put("ackPosition", eventPositions.get(1))
                .done()
            .sendAndAwait();

        // then
        Thread.sleep(1000L); // there might be more received in case this feature is broken
        assertThat(apiRule.numSubscribedEventsAvailable()).isEqualTo(2);

        final List<Long> eventPositionsAfterAck = apiRule.subscribedEvents()
                .limit(2)
                .map((e) -> e.position())
                .collect(Collectors.toList());

        assertThat(eventPositionsAfterAck.get(0)).isGreaterThan(eventPositions.get(2));
        assertThat(eventPositionsAfterAck.get(1)).isGreaterThan(eventPositions.get(2));
    }

    @Test
    public void shouldPushAllEventsWithoutPrefetchCapacity() throws InterruptedException
    {
        // given
        final int nrOfTasks = 5;
        final int prefetchCapacity = -1;

        createTasks(nrOfTasks);

        // when
        openSubscription(prefetchCapacity);

        // then
        final int expectedNumberOfEvents =
                nrOfTasks * 2 + // CREATE and CREATED
                2; // two raft events

        TestUtil.waitUntil(() -> apiRule.numSubscribedEventsAvailable() == expectedNumberOfEvents);
    }

    protected void createTasks(int nrOfTasks)
    {
        for (int i = 0; i < nrOfTasks; i++)
        {
            apiRule.createCmdRequest()
                .topicName(DEFAULT_TOPIC_NAME)
                .partitionId(0)
                .eventTypeTask()
                .command()
                    .put("eventType", "CREATE")
                    .put("type", "theTaskType")
                    .done()
                .sendAndAwait();
        }

    }
}