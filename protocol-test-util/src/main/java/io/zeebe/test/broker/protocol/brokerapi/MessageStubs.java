/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.test.broker.protocol.brokerapi;

import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.MessageIntent;
import java.util.function.Consumer;

public class MessageStubs {

  private StubBrokerRule broker;

  public MessageStubs(StubBrokerRule broker) {
    this.broker = broker;
  }

  public void registerPublishCommand() {
    registerPublishCommand(r -> {});
  }

  public void registerPublishCommand(Consumer<ExecuteCommandResponseBuilder> modifier) {
    final ExecuteCommandResponseBuilder builder =
        broker
            .onExecuteCommandRequest(ValueType.MESSAGE, MessageIntent.PUBLISH)
            .respondWith()
            .event()
            .intent(MessageIntent.PUBLISHED)
            .key(r -> r.key())
            .value()
            .allOf((r) -> r.getCommand())
            .done();

    modifier.accept(builder);

    builder.register();
  }
}