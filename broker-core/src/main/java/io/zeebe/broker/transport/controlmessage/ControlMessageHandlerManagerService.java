/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.transport.controlmessage;

import io.zeebe.broker.clustering.base.topology.RequestTopologyHandler;
import io.zeebe.broker.clustering.base.topology.TopologyManager;
import io.zeebe.dispatcher.Dispatcher;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.transport.ClientTransport;
import io.zeebe.transport.ServerOutput;
import io.zeebe.transport.ServerTransport;
import io.zeebe.util.sched.ActorScheduler;
import java.util.Arrays;
import java.util.List;

public class ControlMessageHandlerManagerService implements Service<ControlMessageHandlerManager> {
  protected final Injector<ServerTransport> transportInjector = new Injector<>();
  protected final Injector<ClientTransport> managementClientTransportInjector = new Injector<>();
  protected final Injector<Dispatcher> controlMessageBufferInjector = new Injector<>();
  private final Injector<TopologyManager> topologyManagerInjector = new Injector<>();

  protected ControlMessageHandlerManager service;

  @Override
  public void start(ServiceStartContext context) {
    final Dispatcher controlMessageBuffer = controlMessageBufferInjector.getValue();

    final ServerTransport transport = transportInjector.getValue();
    final ActorScheduler actorScheduler = context.getScheduler();

    final TopologyManager topologyManager = topologyManagerInjector.getValue();

    final ServerOutput output = transport.getOutput();

    final List<ControlMessageHandler> controlMessageHandlers =
        Arrays.asList(new RequestTopologyHandler(output, topologyManager));

    service =
        new ControlMessageHandlerManager(
            transport.getOutput(), controlMessageBuffer, actorScheduler, controlMessageHandlers);

    context.async(service.openAsync());
  }

  @Override
  public void stop(ServiceStopContext context) {
    context.async(service.closeAsync());
  }

  @Override
  public ControlMessageHandlerManager get() {
    return service;
  }

  public Injector<ServerTransport> getTransportInjector() {
    return transportInjector;
  }

  public Injector<Dispatcher> getControlMessageBufferInjector() {
    return controlMessageBufferInjector;
  }

  public Injector<TopologyManager> getTopologyManagerInjector() {
    return topologyManagerInjector;
  }

  public Injector<ClientTransport> getManagementClientTransportInjector() {
    return managementClientTransportInjector;
  }
}
