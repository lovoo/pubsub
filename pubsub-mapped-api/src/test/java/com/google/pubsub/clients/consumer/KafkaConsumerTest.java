package com.google.pubsub.clients.consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import com.google.pubsub.common.ChannelUtil;
import com.google.pubsub.kafkastubs.common.KafkaException;
import com.google.pubsub.kafkastubs.common.serialization.IntegerSerializer;
import com.google.pubsub.kafkastubs.common.serialization.StringSerializer;
import com.google.pubsub.kafkastubs.consumer.ConsumerRecord;
import com.google.pubsub.kafkastubs.consumer.ConsumerRecords;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.DeleteSubscriptionRequest;
import com.google.pubsub.v1.ListTopicsRequest;
import com.google.pubsub.v1.ListTopicsResponse;
import com.google.pubsub.v1.PublisherGrpc.PublisherImplBase;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SubscriberGrpc;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberFutureStub;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberImplBase;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;


@RunWith(PowerMockRunner.class)
@PrepareForTest(Futures.class)
@SuppressStaticInitializationFor("com.google.pubsub.common.ChannelUtil")
public class KafkaConsumerTest {


  @Rule
  public final GrpcServerRule grpcServerRule = new GrpcServerRule().directExecutor();

  private KafkaConsumer<Integer, String> consumer;

  @Before
  public void setUp() throws ExecutionException, InterruptedException {
    grpcServerRule.getServiceRegistry().addService(new SubscriberImpl());

    ConsumerConfig config = getConsumerConfig();

    PowerMockito.mockStatic(Futures.class);
    when(Futures.allAsList(any(ArrayList.class))).thenReturn(mock(ListenableFuture.class));

    ChannelUtil channelUtil = mock(ChannelUtil.class);
    when(channelUtil.getChannel()).thenReturn(grpcServerRule.getChannel());
    when(channelUtil.getCallCredentials()).thenReturn(null);

    consumer = new KafkaConsumer<>(config, null,
        null, channelUtil);
  }

  @Test
  public void testSubscribe() {
    grpcServerRule.getServiceRegistry().addService(new SubscriberImpl());

    SubscriberFutureStub subscriberFutureStub = SubscriberGrpc
        .newFutureStub(grpcServerRule.getChannel());

    subscriberFutureStub.createSubscription(Subscription.newBuilder().build());
    Set<String> topics = new HashSet<>(Arrays.asList("topic1", "topic2"));
    consumer.subscribe(Arrays.asList("topic2", "topic1"));

    Set<String> subscribed = consumer.subscription();
    assertTrue(topics.equals(subscribed));
  }

  @Test
  public void testUnsubscribe() {
    grpcServerRule.getServiceRegistry().addService(new SubscriberImpl());

    consumer.subscribe(Arrays.asList("topic2", "topic1"));
    consumer.unsubscribe();

    assertTrue(consumer.subscription().isEmpty());
  }

  @Test
  public void testResubscribe() {
    grpcServerRule.getServiceRegistry().addService(new SubscriberImpl());

    Set<String> topics = new HashSet<>(Arrays.asList("topic4", "topic3"));

    consumer.subscribe(Arrays.asList("topic2", "topic1"));
    consumer.subscribe(Arrays.asList("topic3", "topic4"));

    Set<String> subscribed = consumer.subscription();
    assertTrue(topics.equals(subscribed));
  }

  @Test
  public void testSubscribeWithRecurringTopics() {
    grpcServerRule.getServiceRegistry().addService(new SubscriberImpl());

    Set<String> topics = new HashSet<>(Arrays.asList("topic", "topic1"));

    consumer.subscribe(Arrays.asList("topic", "topic1", "topic"));

    Set<String> subscribed = consumer.subscription();
    assertTrue(topics.equals(subscribed));
  }

  @Test
  public void testUnsubscribeSubscriptionDeleted() {
    grpcServerRule.getServiceRegistry().addService(new SubscriberImpl());

    Set<String> topics = new HashSet<>(Arrays.asList("topic", "topic1"));

    consumer.subscribe(Arrays.asList("topic", "topic1", "topic"));

    Set<String> subscribed = consumer.subscription();
    assertTrue(topics.equals(subscribed));
  }

  @Test
  public void testPatternSubscription() {
    grpcServerRule.getServiceRegistry().addService(new SubscriberImpl());
    grpcServerRule.getServiceRegistry().addService(new PublisherImpl());

    Pattern pattern = Pattern.compile("[a-z]*\\d{3}cat");

    consumer.subscribe(pattern, null);
    Set<String> subscribed = consumer.subscription();
    Set<String> expectedTopics = new HashSet<>(Arrays.asList("thisis123cat",
        "funnycats000cat"));
    assertEquals(expectedTopics, subscribed);
  }

  @Test
  public void emptyPatternFails() {
    try {
      Pattern pattern = null;
      consumer.subscribe(pattern, null);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Topic pattern to subscribe to cannot be null", e.getMessage());
    }
  }

  @Test
  public void nullTopicListFails() {
    try {
      List<String> topics = null;
      consumer.subscribe(topics);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Topic collection to subscribe to cannot be null", e.getMessage());
    }
  }

  @Test
  public void emptyTopicFails() {
    try {
      List<String> topics = new ArrayList<>(Collections.singletonList("    "));
      consumer.subscribe(topics);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Topic collection to subscribe to cannot contain null or empty topic",
          e.getMessage());
    }
  }

  @Test
  public void noSubscriptionsPollFails() {
    try {
      consumer.poll(100);
      fail();
    } catch (IllegalStateException e) {
      assertEquals("Consumer is not subscribed to any topics", e.getMessage());
    }
  }

  @Test
  public void negativePollTimeoutFails() {
    try {
      consumer.poll(-200);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Timeout must not be negative", e.getMessage());
    }
  }


  @Test
  public void testDeserializeProperly() throws ExecutionException, InterruptedException {
    grpcServerRule.getServiceRegistry().addService(new SubscriberImpl());

    String topic = "topic";
    Integer key = 125;
    String value = "value";
    consumer.subscribe(Collections.singletonList(topic));

    ConsumerRecords<Integer, String> consumerRecord = consumer.poll(100);

    assertEquals(consumerRecord.count(), 1);
    Iterable<ConsumerRecord<Integer, String>> recordsForTopic =
        consumerRecord.records("topic");

    ConsumerRecord<Integer, String> next = recordsForTopic.iterator().next();

    assertEquals(value, next.value());
    assertEquals(key, next.key());
  }

  @Test
  public void testPollExecutionException() throws ExecutionException, InterruptedException {
    grpcServerRule.getServiceRegistry().addService(new ErrorSubscriberImpl());

    String topic = "topic";
    consumer.subscribe(Collections.singletonList(topic));
    try {
      consumer.poll(100);
      fail();
    } catch (KafkaException e) {
    }
  }

  private ConsumerConfig getConsumerConfig() {
    Properties properties = new Properties();
    properties.putAll(new ImmutableMap.Builder<>()
        .put("key.deserializer",
            "com.google.pubsub.kafkastubs.common.serialization.IntegerDeserializer")
        .put("value.deserializer",
            "com.google.pubsub.kafkastubs.common.serialization.StringDeserializer")
        .put("max.poll.records", 500)
        .build()
    );

    return new ConsumerConfig(
        ConsumerConfig.addDeserializerToConfig(properties, null, null));
  }

  static class SubscriberImpl extends SubscriberImplBase {

    @Override
    public void createSubscription(Subscription request, StreamObserver<Subscription> responseObserver) {
      Subscription s = Subscription.newBuilder().setName("name")
          .setTopic("projects/null/topics/topic").build();
      responseObserver.onNext(s);
      responseObserver.onCompleted();
    }

    @Override
    public void deleteSubscription(DeleteSubscriptionRequest request, StreamObserver<Empty> responseObserver) {
      responseObserver.onCompleted();
    }

    @Override
    public void pull(PullRequest request, StreamObserver<PullResponse> responseObserver) {
      String topic = "topic";
      Integer key = 125;
      String value = "value";
      byte[] serializedKeyBytes = new IntegerSerializer().serialize(topic, key);
      String serializedKey = new String(serializedKeyBytes, StandardCharsets.UTF_8);
      byte[] serializedValueBytes = new StringSerializer().serialize(topic, value);

      Timestamp timestamp = Timestamp.newBuilder().setSeconds(1500).build();

      PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
          .setPublishTime(timestamp)
          .putAttributes("key_attribute", serializedKey)
          .setData(ByteString.copyFrom(serializedValueBytes))
          .build();

      ReceivedMessage receivedMessage = ReceivedMessage.newBuilder()
          .setMessage(pubsubMessage)
          .build();

      PullResponse pullResponse = PullResponse.newBuilder()
          .addReceivedMessages(receivedMessage)
          .build();

      responseObserver.onNext(pullResponse);
      responseObserver.onCompleted();
    }

    @Override
    public void acknowledge(AcknowledgeRequest request, StreamObserver<Empty> responseObserver) {
      Empty empty = Empty.getDefaultInstance();
      responseObserver.onNext(empty);
      responseObserver.onCompleted();
    }
  }

  static class PublisherImpl extends PublisherImplBase {

    @Override
    public void listTopics(ListTopicsRequest request, StreamObserver<ListTopicsResponse> responseObserver) {
      List<String> topicNames = new ArrayList<>(Arrays.asList(
        "projects/null/topics/thisis123cat", "projects/null/topics/abc12345bad",
        "projects/null/topics/noWay1234", "projects/null/topics/funnycats000cat"));
      List<Topic> topics = new ArrayList<>();

      for (String topicName: topicNames) {
        topics.add(Topic.newBuilder().setName(topicName).build());
      }

      ListTopicsResponse listTopicsResponse = ListTopicsResponse.newBuilder()
          .addAllTopics(topics).build();

      responseObserver.onNext(listTopicsResponse);
      responseObserver.onCompleted();
    }
  }

  static class ErrorSubscriberImpl extends SubscriberImpl {

    @Override
    public void pull(PullRequest request, StreamObserver<PullResponse> responseObserver) {
      ExecutionException executionException = new ExecutionException(new Throwable("message"));
      responseObserver.onError(executionException);
    }

    @Override
    public void acknowledge(AcknowledgeRequest request, StreamObserver<Empty> responseObserver) {
      responseObserver.onError(new Throwable("This test should not have called ack"));
    }
  }
}