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

  public final String ENDPOINT;
  public final String TOPIC_PREFIX;
  public final String PROJECT_PREFIX;

  private ManagedChannel channel;
  private CallCredentials credentials;

  public static ChannelUtil instance = new ChannelUtil();

  private ChannelUtil() {
    TOPIC_PREFIX = "topics/";
    PROJECT_PREFIX = "projects/";
    ENDPOINT = "pubsub.googleapis.com";

    channel = ManagedChannelBuilder.forTarget(ENDPOINT).build();
    try {
      credentials = MoreCallCredentials.from(GoogleCredentials.getApplicationDefault());
    } catch (Exception exception) {
      System.out.println("Couldn't retrieve credentials!");
    }
  }

  public void reinitializeChannel() {
    channel = ManagedChannelBuilder.forTarget(ENDPOINT).build();
  }

  public PublisherStub getAsyncStub() {
    return PublisherGrpc.newStub(channel).withCallCredentials(credentials);
  }

  public PublisherFutureStub getFutureStub() {
    return PublisherGrpc.newFutureStub(channel).withCallCredentials(credentials);
  }

  public PublisherBlockingStub getBlockingStub() {
    return PublisherGrpc.newBlockingStub(channel).withCallCredentials(credentials);
  }
}