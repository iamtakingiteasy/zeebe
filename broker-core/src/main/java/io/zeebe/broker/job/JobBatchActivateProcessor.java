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
package io.zeebe.broker.job;

import static io.zeebe.util.sched.clock.ActorClock.currentTimeMillis;

import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedRecordProcessor;
import io.zeebe.broker.logstreams.processor.TypedResponseWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.workflow.state.WorkflowState;
import io.zeebe.msgpack.value.StringValue;
import io.zeebe.msgpack.value.ValueArray;
import io.zeebe.protocol.clientapi.RejectionType;
import io.zeebe.protocol.impl.record.value.job.JobBatchRecord;
import io.zeebe.protocol.impl.record.value.job.JobRecord;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.intent.JobBatchIntent;
import io.zeebe.protocol.intent.JobIntent;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.ObjectHashSet;
import org.agrona.concurrent.UnsafeBuffer;

public class JobBatchActivateProcessor implements TypedRecordProcessor<JobBatchRecord> {

  private final JobState state;
  private final WorkflowState workflowState;
  private final ObjectHashSet<DirectBuffer> variableNames = new ObjectHashSet<>();

  public JobBatchActivateProcessor(JobState state, WorkflowState workflowState) {
    this.state = state;
    this.workflowState = workflowState;
  }

  @Override
  public void processRecord(
      final TypedRecord<JobBatchRecord> record,
      final TypedResponseWriter responseWriter,
      final TypedStreamWriter streamWriter) {
    final JobBatchRecord value = record.getValue();
    if (isValid(value)) {
      activateJobs(record, responseWriter, streamWriter);
    } else {
      rejectCommand(record, responseWriter, streamWriter);
    }
  }

  private boolean isValid(final JobBatchRecord record) {
    return record.getAmount() > 0
        && record.getTimeout() > 0
        && record.getType().capacity() > 0
        && record.getWorker().capacity() > 0;
  }

  private void activateJobs(
      final TypedRecord<JobBatchRecord> record,
      final TypedResponseWriter responseWriter,
      final TypedStreamWriter streamWriter) {
    final JobBatchRecord value = record.getValue();

    final long jobBatchKey = streamWriter.getKeyGenerator().nextKey();

    final AtomicInteger amount = new AtomicInteger(value.getAmount());

    // TODO: put variable names in set here
    variableNames.clear();
    final ValueArray<StringValue> variables = value.variables();

    variables.forEach(
        v -> {
          final MutableDirectBuffer nameCopy = new UnsafeBuffer(new byte[v.getValue().capacity()]);
          nameCopy.putBytes(0, v.getValue(), 0, v.getValue().capacity());
          variableNames.add(nameCopy);
        });

    state.forEachActivatableJobs(
        value.getType(),
        (key, jobRecord, control) -> {
          final int remainingAmount = amount.decrementAndGet();
          if (remainingAmount >= 0) {
            final long deadline = currentTimeMillis() + value.getTimeout();
            value.jobKeys().add().setValue(key);
            final JobRecord job = value.jobs().add();

            // clone job record to modify it
            final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(jobRecord.getLength());
            jobRecord.write(buffer, 0);
            job.wrap(buffer);

            // set worker properties on job
            job.setDeadline(deadline).setWorker(value.getWorker());

            final long elementInstanceKey = job.getHeaders().getElementInstanceKey();

            if (elementInstanceKey >= 0) {
              final DirectBuffer payload = collectPayload(variableNames, elementInstanceKey);
              job.setPayload(payload);
            } else {
              job.setPayload(WorkflowInstanceRecord.EMPTY_PAYLOAD);
            }

            // update state and write follow up event for job record
            state.activate(key, job);
            streamWriter.appendFollowUpEvent(key, JobIntent.ACTIVATED, job);
          }

          if (remainingAmount < 1) {
            control.stop();
          }
        });

    streamWriter.appendFollowUpEvent(jobBatchKey, JobBatchIntent.ACTIVATED, value);
    responseWriter.writeEventOnCommand(jobBatchKey, JobBatchIntent.ACTIVATED, value, record);
  }

  private DirectBuffer collectPayload(
      Collection<DirectBuffer> variableNames, long elementInstanceKey) {
    final DirectBuffer payload;
    if (variableNames.isEmpty()) {
      payload =
          workflowState
              .getElementInstanceState()
              .getVariablesState()
              .getVariablesAsDocument(elementInstanceKey);
    } else {
      payload =
          workflowState
              .getElementInstanceState()
              .getVariablesState()
              .getVariablesAsDocument(elementInstanceKey, variableNames);
    }
    return payload;
  }

  private void rejectCommand(
      final TypedRecord<JobBatchRecord> record,
      final TypedResponseWriter responseWriter,
      final TypedStreamWriter streamWriter) {
    final RejectionType rejectionType;
    final String rejectionReason;

    final JobBatchRecord value = record.getValue();

    if (value.getAmount() < 1) {
      rejectionType = RejectionType.BAD_VALUE;
      rejectionReason = "Job batch amount must be greater than zero, got " + value.getAmount();
    } else if (value.getTimeout() < 1) {
      rejectionType = RejectionType.BAD_VALUE;
      rejectionReason = "Job batch timeout must be greater than zero, got " + value.getTimeout();
    } else if (value.getType().capacity() < 1) {
      rejectionType = RejectionType.BAD_VALUE;
      rejectionReason = "Job batch type must not be empty";
    } else if (value.getWorker().capacity() < 1) {
      rejectionType = RejectionType.BAD_VALUE;
      rejectionReason = "Job batch worker must not be empty";
    } else {
      throw new IllegalStateException("Job batch command is valid and should not be rejected");
    }

    streamWriter.appendRejection(record, rejectionType, rejectionReason);
    responseWriter.writeRejectionOnCommand(record, rejectionType, rejectionReason);
  }
}
