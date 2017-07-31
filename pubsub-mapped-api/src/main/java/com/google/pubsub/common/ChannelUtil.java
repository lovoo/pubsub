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

package com.google.pubsub.common;

import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.PublisherGrpc.PublisherStub;
import com.google.pubsub.v1.PublisherGrpc.PublisherFutureStub;
import com.google.pubsub.v1.PublisherGrpc.PublisherBlockingStub;

import io.grpc.ManagedChannel;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannelBuilder;
import io.grpc.auth.MoreCallCredentials;
import com.google.auth.oauth2.GoogleCredentials;

/**
 * Sets up the pub/sub grpc functionality
 */
public class ChannelUtil {

  private ChannelUtil() {}
  private static ManagedChannel channel;
  private static CallCredentials credentials;

  private static final String ENDPOINT;
  public static final String TOPIC_PREFIX;
  public static final String PROJECT_PREFIX;

  static {
    TOPIC_PREFIX = "topics/";
    PROJECT_PREFIX = "projects/";
    ENDPOINT = "pubsub.googleapis.com";
    channel = ManagedChannelBuilder.forTarget(ENDPOINT).build();
    try {
      credentials = MoreCallCredentials.from(GoogleCredentials.getApplicationDefault());
    } catch (Exception exception) {
      System.out.println("COULDN'T RETRIEVE CREDENTIALS");
    }
  }

  private static void reinitializeChannel() { channel = ManagedChannelBuilder.forTarget(ENDPOINT).build(); }

  public static PublisherStub getAsyncStub() {
    if (isChannelDown())
      reinitializeChannel();
    return PublisherGrpc.newStub(channel).withCallCredentials(credentials);
  }

  public static PublisherBlockingStub getBlockingStub() {
    if (isChannelDown())
      reinitializeChannel();
    return PublisherGrpc.newBlockingStub(channel).withCallCredentials(credentials);
  }

  public static PublisherFutureStub getFutureStub() {
    if (isChannelDown())
      reinitializeChannel();
    return PublisherGrpc.newFutureStub(channel).withCallCredentials(credentials);
  }

  public static void closeChannel() { channel.shutdown(); }

  public static void closeChannelNow() { channel.shutdownNow(); }

  public static boolean isChannelDown() { return channel.isShutdown(); }
}