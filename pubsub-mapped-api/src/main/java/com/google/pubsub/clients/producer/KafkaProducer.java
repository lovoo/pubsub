// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////////

package com.google.pubsub.clients.producer;

import com.google.protobuf.ByteString;
import com.google.pubsub.common.ChannelUtil;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PublisherGrpc.PublisherFutureStub;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Collections;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.Serializer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Set;
import java.util.Map;
import java.util.List;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Kafka client that publishes records to Google Cloud Pub/Sub.
 */
public class KafkaProducer<K, V> implements Producer<K, V> {

  private final String TOPIC_PREFIX = "topics/";
  private final String PROJECT_PREFIX = "projects/";

  private PublisherFutureStub publisher;
  private ChannelUtil channelUtil = ChannelUtil.instance;
  private Set<ListenableFuture<PublishResponse>> requests;

  private String topic;
  private String project;
  private Serializer<K> keySerializer;
  private Serializer<V> valueSerializer;
  private ProducerConfig producerConfig;
  private AtomicBoolean lock;
  private AtomicBoolean closed;

  private long lingerMs;
  private boolean isAcks;
  private int batchSize;
  private int maxRequestSize;
  private CompressionType compressionType;

  public KafkaProducer(Map<String, Object> configs) {
    this(new ProducerConfig(configs), null, null);
  }

  public KafkaProducer(Map<String, Object> configs,
      Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    this(new ProducerConfig(ProducerConfig.addSerializerToConfig(configs, keySerializer, valueSerializer)),
        keySerializer, valueSerializer);
  }

  public KafkaProducer(Properties properties) {
    this(new ProducerConfig(properties), null, null);
  }

  public KafkaProducer(Properties properties, Serializer<K> keySerializer,
      Serializer<V> valueSerializer) {
    this(new ProducerConfig(ProducerConfig.addSerializerToConfig(properties, keySerializer, valueSerializer)),
        keySerializer, valueSerializer);
  }

  @SuppressWarnings("unchecked")
  private KafkaProducer(ProducerConfig configs, Serializer<K> keySerializer,
      Serializer<V> valueSerializer) {
    try {
      this.producerConfig = configs;
      this.requests = Collections.synchronizedSet(new HashSet<ListenableFuture<PublishResponse>>());

      this.topic = configs.getString(ProducerConfig.TOPIC_CONFIG);
      this.project = configs.getString(ProducerConfig.PROJECT_CONFIG);

      if (keySerializer == null) {
        this.keySerializer = configs.getConfiguredInstance(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            Serializer.class);
        this.keySerializer.configure(configs.originals(), true);
      } else {
        configs.ignore(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        this.keySerializer = keySerializer;
      }

      if (valueSerializer == null) {
        this.valueSerializer = configs.getConfiguredInstance(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            Serializer.class);
        this.valueSerializer.configure(configs.originals(), false);
      } else {
        configs.ignore(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        this.valueSerializer = valueSerializer;
      }

      this.lock = new AtomicBoolean(false);
      this.closed = new AtomicBoolean(false);
      this.lingerMs = configs.getLong(ProducerConfig.LINGER_MS_CONFIG);
      this.batchSize = configs.getInt(ProducerConfig.BATCH_SIZE_CONFIG);
      this.maxRequestSize = configs.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
      this.isAcks = configs.getString(ProducerConfig.ACKS_CONFIG).matches("1|all");
      this.compressionType = CompressionType.forName(configs.getString(ProducerConfig.COMPRESSION_TYPE_CONFIG));

    } catch (Exception e) {
      close(0, TimeUnit.MILLISECONDS);
      throw new RuntimeException("Failed to construct kafka producer.", e);
    }
  }

  //TODO: Remove after Testing.

  public KafkaProducer(ProducerConfig configs, Serializer<K> keySerializer,
      Serializer<V> valueSerializer, ChannelUtil util) {
    this(configs, keySerializer, valueSerializer);

    channelUtil = util;
    publisher = util.getFutureStub();
  }

  /**
   * Sends the given record.
   */
  public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
    return send(record, null);
  }

  //TODO: Introduce deadline, retries, batching and linger.

  /**
   * Sends the given record and invokes the specified callback.
   * The given record must have the same topic as the producer.
   */
  public Future<RecordMetadata> send(ProducerRecord<K, V> record, final Callback callback) {
    while (lock.get());

    if (closed.get())
      throw new RuntimeException("Publisher is closed.");

    if (!topic.equals(record.topic()))
      throw new RuntimeException("Topic doesn't match the pre-assigned one.");

    if (publisher == null)
      publisher = channelUtil.getFutureStub();

    Map<String, String> attributes = new HashMap<>();

    byte[] keyBytes = ByteString.EMPTY.toByteArray();
    if (record.key() != null) {
      keyBytes = keySerializer.serialize(topic, record.key());
    }
    attributes.put("key", new String(keyBytes));

    byte[] valueBytes = ByteString.EMPTY.toByteArray();
    if (record.value() != null) {
      valueBytes = valueSerializer.serialize(topic, record.value());
    }

    PubsubMessage msg = PubsubMessage.newBuilder()
        .setData(ByteString.copyFrom(valueBytes))
        .putAllAttributes(attributes)
        .build();

    PublishRequest request = PublishRequest.newBuilder()
        .setTopic(PROJECT_PREFIX + project +
            "/" + TOPIC_PREFIX + topic)
        .addMessages(msg)
        .build();

    final RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition(topic, 0)
        , 0L, 0L, System.currentTimeMillis()
        , 0L, keyBytes.length, valueBytes.length);

    final ListenableFuture<PublishResponse> response = publisher.publish(request);

    requests.add(response);

    Futures.addCallback(response, new FutureCallback<PublishResponse>() {
      @Override
      public void onSuccess(PublishResponse publishResponse) {
        requests.remove(response);
        if (callback != null)
          callback.onCompletion(recordMetadata, null);
      }

      @Override
      public void onFailure(Throwable throwable) {
        requests.remove(response);
        if (callback != null)
          callback.onCompletion(recordMetadata, new ExecutionException(throwable));
      }
    });

    SettableFuture<RecordMetadata> future = SettableFuture.create();
    future.set(recordMetadata);
    return future;
  }

  /**
   * Iterate over all futures and check if they finished - block on their completion -
   * or just restart the channel - you choose -
   */
  public void flush() {
    lock.set(true);
    while(!requests.isEmpty());
    lock.set(false);
  }

  /**
   * It will always return a list of 1 element which is <0, "", 0>.
   */
  public List<PartitionInfo> partitionsFor(String topic) {
    Node[] dummy = {new Node(0, "", 0)};
    return Arrays.asList(new PartitionInfo(topic, 0, dummy[0], dummy, dummy));
  }

  /**
   * Returns an empty HashMap.
   */
  public Map<MetricName, ? extends Metric> metrics() { return new HashMap<>(); }

  /**
   * Closes the producer.
   */
  public void close() { close(0, null); }

  //TODO: Handle the timeout part.

  /**
   * Closes the producer with the given timeout.
   */
  public void close(long timeout, TimeUnit unit) {
    if (timeout < 0)
      throw new IllegalArgumentException("Wrong parameter: Time cannot be negative.");

    if (closed.getAndSet(true))
      throw new IllegalStateException("Producer already closed.");

    flush();
    keySerializer.close();
    valueSerializer.close();
  }
}