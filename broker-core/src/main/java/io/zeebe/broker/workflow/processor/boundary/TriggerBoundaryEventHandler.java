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
package io.zeebe.broker.workflow.processor.boundary;

import io.zeebe.broker.incident.data.ErrorType;
import io.zeebe.broker.workflow.model.element.ExecutableBoundaryEvent;
import io.zeebe.broker.workflow.processor.BpmnStepContext;
import io.zeebe.broker.workflow.processor.BpmnStepHandler;
import io.zeebe.broker.workflow.processor.flownode.IOMappingHelper;
import io.zeebe.broker.workflow.state.WorkflowState;
import io.zeebe.msgpack.mapping.MappingException;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import org.agrona.DirectBuffer;

public class TriggerBoundaryEventHandler implements BpmnStepHandler<ExecutableBoundaryEvent> {

  private final IOMappingHelper ioMappingHelper = new IOMappingHelper();
  private final WorkflowState state;

  public TriggerBoundaryEventHandler(WorkflowState state) {
    this.state = state;
  }

  @Override
  public void handle(BpmnStepContext<ExecutableBoundaryEvent> context) {
    try {
      final long key = context.getRecord().getKey();

      final DirectBuffer mappedPayload = ioMappingHelper.applyOutputMappings(context);
      context.getValue().setPayload(mappedPayload);

      // TODO: nice, method chaining
      state
          .getElementInstanceState()
          .getVariablesState()
          .setVariablesFromDocument(key, mappedPayload);

      context
          .getOutput()
          .appendFollowUpEvent(
              key, WorkflowInstanceIntent.CATCH_EVENT_TRIGGERED, context.getValue());
    } catch (MappingException e) {
      context.raiseIncident(ErrorType.IO_MAPPING_ERROR, e.getMessage());
    }
  }
}
