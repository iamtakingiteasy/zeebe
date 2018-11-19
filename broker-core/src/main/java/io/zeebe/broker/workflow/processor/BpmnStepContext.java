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
package io.zeebe.broker.workflow.processor;

import io.zeebe.broker.incident.data.ErrorType;
import io.zeebe.broker.incident.data.IncidentRecord;
import io.zeebe.broker.logstreams.processor.TypedCommandWriter;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamWriterImpl;
import io.zeebe.broker.workflow.model.element.ExecutableFlowElement;
import io.zeebe.broker.workflow.state.ElementInstance;
import io.zeebe.msgpack.mapping.MsgPackMergeTool;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.intent.IncidentIntent;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;

public class BpmnStepContext<T extends ExecutableFlowElement> {

  private final IncidentRecord incidentCommand = new IncidentRecord();
  private final SideEffectQueue sideEffect = new SideEffectQueue();
  private final EventOutput eventOutput;
  private final MsgPackMergeTool mergeTool;
  private final CatchEventOutput catchEventOutput;

  private TypedRecord<WorkflowInstanceRecord> record;
  private ExecutableFlowElement element;
  private TypedCommandWriter commandWriter;

  private ElementInstance flowScopeInstance;
  private ElementInstance elementInstance;

  public BpmnStepContext(EventOutput eventOutput, CatchEventOutput catchEventOutput) {
    this.eventOutput = eventOutput;
    this.mergeTool = new MsgPackMergeTool(4096);
    this.catchEventOutput = catchEventOutput;
  }

  public TypedRecord<WorkflowInstanceRecord> getRecord() {
    return record;
  }

  public WorkflowInstanceRecord getValue() {
    return record.getValue();
  }

  public WorkflowInstanceIntent getState() {
    return (WorkflowInstanceIntent) record.getMetadata().getIntent();
  }

  public void setRecord(final TypedRecord<WorkflowInstanceRecord> record) {
    this.record = record;
  }

  public T getElement() {
    return (T) element;
  }

  public void setElement(final ExecutableFlowElement element) {
    this.element = element;
  }

  public EventOutput getOutput() {
    return eventOutput;
  }

  public void setStreamWriter(final TypedStreamWriter streamWriter) {
    this.eventOutput.setStreamWriter(streamWriter);
    this.commandWriter = streamWriter;
  }

  public MsgPackMergeTool getMergeTool() {
    return mergeTool;
  }

  public TypedCommandWriter getCommandWriter() {
    return commandWriter;
  }

  public CatchEventOutput getCatchEventOutput() {
    return catchEventOutput;
  }

  public ElementInstance getFlowScopeInstance() {
    return flowScopeInstance;
  }

  public void setFlowScopeInstance(final ElementInstance flowScopeInstance) {
    this.flowScopeInstance = flowScopeInstance;
  }

  /**
   * can be null
   *
   * @return
   */
  public ElementInstance getElementInstance() {
    return elementInstance;
  }

  public void setElementInstance(final ElementInstance elementInstance) {
    this.elementInstance = elementInstance;
  }

  public SideEffectQueue getSideEffect() {
    return sideEffect;
  }

  public void raiseIncident(ErrorType errorType, String errorMessage) {
    incidentCommand.reset();

    incidentCommand
        .initFromWorkflowInstanceFailure(record)
        .setErrorType(errorType)
        .setErrorMessage(errorMessage);

    eventOutput.storeFailedToken(record);

    if (!record.getMetadata().hasIncidentKey()) {
      commandWriter.appendNewCommand(IncidentIntent.CREATE, incidentCommand);
    } else {
      // TODO: casting is ok for the moment; the problem is rather that we
      // write an event (not command) for a different stream processor
      // => https://github.com/zeebe-io/zeebe/issues/1033
      ((TypedStreamWriterImpl) commandWriter)
          .appendFollowUpEvent(
              record.getMetadata().getIncidentKey(),
              IncidentIntent.RESOLVE_FAILED,
              incidentCommand);
    }
  }
}
