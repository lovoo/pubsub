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

package com.google.pubsub.driver;

import java.util.Properties;
import com.google.pubsub.common.ChannelUtil;
import com.google.pubsub.clients.producer.KafkaProducer;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Main {

  public static void main(String[] args) {

    Properties props = new Properties();
    props.put("topic", "wow");
    props.put("project", "dataproc-kafka-test");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    final KafkaProducer<String, String> producer = new KafkaProducer<>(props);

    for (int i = 0; i < 10; i++) {
      producer.send(new ProducerRecord<>(
          "wow", 0, Integer.toString(i), "msg" + Integer.toString(i)));
      System.out.println(i);
    }

    new Thread(new Runnable() {
      @Override
      public void run() {
        for (int i = 10; i < 20; i++) {
          producer.send(new ProducerRecord<>(
              "wow", 0, Integer.toString(i), "msg" + Integer.toString(i)));
          System.out.println(i);
        }
      }
    }).start();

    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {}
        producer.flush();
      }
    }).start();
  }
}
