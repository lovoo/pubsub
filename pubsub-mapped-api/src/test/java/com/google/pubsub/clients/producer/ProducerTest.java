package com.google.pubsub.clients.producer;

import org.junit.Test;
import org.junit.Before;
import org.junit.Assert;
import org.junit.runner.RunWith;

import org.mockito.Mockito;
import org.mockito.Matchers;
import org.mockito.ArgumentCaptor;

import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;

import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.common.ChannelUtil;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;

import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.PublisherGrpc.PublisherFutureStub;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import java.util.Properties;

/**
 * Created by gewillovic on 8/1/17.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({PublisherGrpc.class, ChannelUtil.class})
@SuppressStaticInitializationFor("com.google.pubsub.common.ChannelUtil")
public class ProducerTest {

  private Deserializer deserializer;
  private PublisherFutureStub futureStub;
  private ArgumentCaptor<PublishRequest> captor;
  private KafkaProducer<String, Integer> publisher;

  @Before
  public void setUp() {
    Properties properties = new Properties();
    properties.putAll(new ImmutableMap.Builder<>()
        .put("topic", "topic")
        .put("project", "project")
        .put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        .put("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
        .build()
    );

    ChannelUtil util = Mockito.mock(ChannelUtil.class);
    futureStub = Mockito.mock(PublisherFutureStub.class);
    captor = ArgumentCaptor.forClass(PublishRequest.class);
    ListenableFuture<PublishResponse> lf = Mockito.mock(ListenableFuture.class);

    ProducerConfig config = new ProducerConfig(
        ProducerConfig.addSerializerToConfig(properties, null, null));

    Mockito.when(util.getFutureStub()).thenReturn(futureStub);
    Mockito.when(futureStub.publish(captor.capture())).thenReturn(lf);

    publisher = new KafkaProducer<>(config, null, null, util);
  }

  @Test
  public void testTopicProjectCorrectness() {
    publisher.send(new ProducerRecord<String, Integer>("topic", 123));

    String[] tokens = captor.getValue().getTopic().split("/");

    Assert.assertEquals("Topic should be the one previously provided.",
        "topic", tokens[tokens.length - 1]);


    Assert.assertEquals("Project should be the one previously provided.",
        "project", tokens[tokens.length - 3]);
  }

  @Test
  public void testSerializers() {
    publisher.send(new ProducerRecord<String, Integer>("topic", 123));

    deserializer = new StringDeserializer();

    Assert.assertEquals("Key should be an empty string.",
        "", deserializer.deserialize("topic",
            captor.getValue().getMessages(0).getAttributesMap().get("key").getBytes()));

    deserializer = new IntegerDeserializer();

    Assert.assertEquals("Value should be the one previously provided.",
        123, deserializer.deserialize("topic",
            captor.getValue().getMessages(0).getData().toByteArray()));
  }

  @Test (expected = RuntimeException.class)
  public void testPublishWrongTopic() {
    publisher.send(new ProducerRecord<String, Integer>("topic2", 123));
  }

  @Test
  public void testNumberOfPublishIssued() {
    publisher.send(new ProducerRecord<String, Integer>("topic", 123));

    Mockito.verify(futureStub, Mockito.times(1)).
        publish(Matchers.<PublishRequest>any());

    publisher.send(new ProducerRecord<String, Integer>("topic", 456));

    Mockito.verify(futureStub, Mockito.times(2)).
        publish(Matchers.<PublishRequest>any());
  }

  @Test
  public void testFlushBlocking() {
    publisher.send(new ProducerRecord<String, Integer>("topic", 123));

    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        publisher.flush();
      }
    });

    thread.start();

    new Thread(new Runnable() {
      @Override
      public void run() {
        publisher.send(new ProducerRecord<String, Integer>("topic", 456));
      }
    }).start();

    try {

      Thread.currentThread().sleep(1000);

      Assert.assertTrue(thread.isAlive());

      Mockito.verify(futureStub, Mockito.times(1)).
          publish(Matchers.<PublishRequest>any());

    } catch (InterruptedException e) {
      throw new RuntimeException("Repeat the test, the thread was interrupted.");
    } catch (AssertionError e) {
      throw new IllegalStateException("Thread should stay alive - blocking on a response -.");
    } catch (RuntimeException e) {
      throw new RuntimeException("The second send should be blocked, and the count should stay 1.");
    }
  }

  @Test
  public void testCloseOnCompletion() {
    publisher.send(new ProducerRecord<String, Integer>("topic", 123));

    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        publisher.close();
      }
    });

    thread.start();

    try {

      Thread.currentThread().sleep(1000);

      Assert.assertTrue(thread.isAlive());

    } catch (InterruptedException e) {
      throw new RuntimeException("Repeat the test, the thread was interrupted.");
    } catch (AssertionError e) {
      throw new IllegalStateException("Thread should stay alive - blocking on a response -.");
    }
  }

  @Test (expected = RuntimeException.class)
  public void testPublishToClosedPublisher() {
    publisher.close();

    publisher.send(new ProducerRecord<String, Integer>("topic", 123));
  }
}
