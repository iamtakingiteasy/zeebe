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
package io.zeebe.broker.workflow;

import static io.zeebe.broker.test.MsgPackConstants.JSON_DOCUMENT;
import static io.zeebe.broker.test.MsgPackConstants.MERGED_OTHER_WITH_JSON_DOCUMENT;
import static io.zeebe.broker.test.MsgPackConstants.MSGPACK_PAYLOAD;
import static io.zeebe.broker.test.MsgPackConstants.NODE_JSON_OBJECT_PATH;
import static io.zeebe.broker.test.MsgPackConstants.NODE_ROOT_PATH;
import static io.zeebe.broker.test.MsgPackConstants.NODE_STRING_PATH;
import static io.zeebe.broker.test.MsgPackConstants.OTHER_DOCUMENT;
import static io.zeebe.broker.test.MsgPackConstants.OTHER_PAYLOAD;
import static io.zeebe.broker.workflow.JobAssert.assertJobPayload;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.broker.test.EmbeddedBrokerRule;
import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.record.value.WorkflowInstanceRecordValue;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.instance.zeebe.ZeebeOutputBehavior;
import io.zeebe.protocol.intent.JobIntent;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import io.zeebe.test.broker.protocol.clientapi.ClientApiRule;
import io.zeebe.test.broker.protocol.clientapi.PartitionTestClient;
import io.zeebe.test.util.MsgPackUtil;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

/** Represents a test class to test the input and output mappings for tasks inside a workflow. */
public class WorkflowTaskIOMappingTest {
  public EmbeddedBrokerRule brokerRule = new EmbeddedBrokerRule();
  public ClientApiRule apiRule = new ClientApiRule(brokerRule::getClientAddress);

  @Rule public RuleChain ruleChain = RuleChain.outerRule(brokerRule).around(apiRule);

  private PartitionTestClient testClient;

  @Before
  public void init() {
    testClient = apiRule.partitionClient();
  }

  @Test
  public void shouldUseDefaultInputMappingIfNoMappingIsSpecified() {
    // given
    testClient.deploy(
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .serviceTask("service", t -> t.zeebeTaskType("external"))
            .endEvent()
            .done());

    // when
    final long workflowInstanceKey = testClient.createWorkflowInstance("process", MSGPACK_PAYLOAD);

    // then
    final Record event = testClient.receiveFirstJobEvent(JobIntent.CREATED);

    assertThat(event.getKey()).isGreaterThan(0).isNotEqualTo(workflowInstanceKey);
    assertJobPayload(event, JSON_DOCUMENT);
  }

  @Test
  public void shouldCreateTwoNewObjectsViaInputMapping() {
    // given
    testClient.deploy(
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .serviceTask(
                "service",
                t ->
                    t.zeebeTaskType("external")
                        .zeebeInput(NODE_STRING_PATH, "$.newFoo")
                        .zeebeInput(NODE_JSON_OBJECT_PATH, "$.newObj"))
            .endEvent()
            .done());

    // when
    testClient.createWorkflowInstance("process", MSGPACK_PAYLOAD);
    final Record event = testClient.receiveFirstJobCommand(JobIntent.CREATE);

    // then payload is expected as
    assertJobPayload(event, "{'newFoo':'value', 'newObj':{'testAttr':'test'}}");
  }

  @Test
  public void shouldUseEmptyObjectIfCreatedWithNoPayload() {
    // given
    testClient.deploy(
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .serviceTask("service", t -> t.zeebeTaskType("external"))
            .endEvent()
            .done());

    // when
    testClient.createWorkflowInstance("process");
    final Record event = testClient.receiveFirstJobCommand(JobIntent.CREATE);

    // then
    assertJobPayload(event, "{}");
  }

  @Test
  public void shouldUseDefaultOutputMapping() {
    // given
    testClient.deploy(
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .serviceTask("service", t -> t.zeebeTaskType("external"))
            .endEvent()
            .done());

    testClient.createWorkflowInstance("process", MSGPACK_PAYLOAD);

    // when
    testClient.completeJobOfType("external", OTHER_PAYLOAD);

    // then
    assertRecordPayload(WorkflowInstanceIntent.ELEMENT_COMPLETED, MERGED_OTHER_WITH_JSON_DOCUMENT);
  }

  @Test
  public void shouldUseDefaultOutputMappingWithNoWorkflowPayload() {
    // given
    testClient.deploy(
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .serviceTask("service", t -> t.zeebeTaskType("external"))
            .endEvent()
            .done());

    testClient.createWorkflowInstance("process");

    // when
    testClient.completeJobOfType("external", OTHER_PAYLOAD);

    // then
    assertRecordPayload(WorkflowInstanceIntent.ELEMENT_COMPLETED, OTHER_DOCUMENT);
  }

  @Test
  public void shouldUseOutputMappingWithNoWorkflowPayload() {
    // given
    testClient.deploy(
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .serviceTask(
                "service", t -> t.zeebeTaskType("external").zeebeOutput("$.string", "$.foo"))
            .endEvent()
            .done());

    testClient.createWorkflowInstance("process");

    // when
    testClient.completeJobOfType("external", OTHER_PAYLOAD);

    // then
    assertRecordPayload(WorkflowInstanceIntent.ELEMENT_COMPLETED, "{'foo':'bar'}");
  }

  @Test
  public void shouldUseNoneOutputBehaviorWithoutCompletePayload() {
    // given
    testClient.deploy(
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .serviceTask(
                "service",
                t -> t.zeebeTaskType("external").zeebeOutputBehavior(ZeebeOutputBehavior.none))
            .endEvent()
            .done());

    testClient.createWorkflowInstance("process", MSGPACK_PAYLOAD);

    // when
    testClient.completeJobOfType("external");

    // then
    assertRecordPayload(WorkflowInstanceIntent.ELEMENT_COMPLETED, JSON_DOCUMENT);
  }

  @Test
  public void shouldUseNoneOutputBehaviorAndCompletePayload() {
    // given
    testClient.deploy(
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .serviceTask(
                "service",
                t -> t.zeebeTaskType("external").zeebeOutputBehavior(ZeebeOutputBehavior.none))
            .endEvent()
            .done());

    testClient.createWorkflowInstance("process", MSGPACK_PAYLOAD);

    // when
    testClient.completeJobOfType("external", OTHER_PAYLOAD);

    // then
    assertRecordPayload(WorkflowInstanceIntent.ELEMENT_COMPLETED, JSON_DOCUMENT);
  }

  @Test
  public void shouldUseOverwriteOutputBehaviorWithoutCompletePayload() {
    // given
    testClient.deploy(
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .serviceTask(
                "service",
                t -> t.zeebeTaskType("external").zeebeOutputBehavior(ZeebeOutputBehavior.overwrite))
            .endEvent()
            .done());

    testClient.createWorkflowInstance("process", MSGPACK_PAYLOAD);

    // when
    testClient.completeJobOfType("external");

    // then
    assertRecordPayload(WorkflowInstanceIntent.ELEMENT_COMPLETED, "{}");
  }

  @Test
  public void shouldUseOverwriteOutputBehaviorAndCompletePayload() {
    // given
    testClient.deploy(
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .serviceTask(
                "service",
                t -> t.zeebeTaskType("external").zeebeOutputBehavior(ZeebeOutputBehavior.overwrite))
            .endEvent()
            .done());

    testClient.createWorkflowInstance("process", MSGPACK_PAYLOAD);

    // when
    testClient.completeJobOfType("external", OTHER_PAYLOAD);

    // then
    assertRecordPayload(WorkflowInstanceIntent.ELEMENT_COMPLETED, OTHER_DOCUMENT);
  }

  @Test
  public void shouldUseOverwriteOutputBehaviorWithOutputMappingAndCompletePayload() {
    // given
    testClient.deploy(
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .serviceTask(
                "service",
                t ->
                    t.zeebeTaskType("external")
                        .zeebeOutputBehavior(ZeebeOutputBehavior.overwrite)
                        .zeebeOutput("$.string", "$.foo"))
            .endEvent()
            .done());

    testClient.createWorkflowInstance("process", MSGPACK_PAYLOAD);

    // when
    testClient.completeJobOfType("external", OTHER_PAYLOAD);

    // then
    assertRecordPayload(WorkflowInstanceIntent.ELEMENT_COMPLETED, "{'foo':'bar'}");
  }

  @Test
  public void shouldUseDefaultOutputMappingWithNoCompletePayload() {
    // given
    testClient.deploy(
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .serviceTask("service", t -> t.zeebeTaskType("external"))
            .endEvent()
            .done());

    testClient.createWorkflowInstance("process", MSGPACK_PAYLOAD);

    // when
    testClient.completeJobOfType("external");

    // then
    assertRecordPayload(WorkflowInstanceIntent.ELEMENT_COMPLETED, JSON_DOCUMENT);
  }

  @Test
  public void shouldUseDefaultOutputMappingWithNoCreatedPayload() {
    // given
    testClient.deploy(
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .serviceTask("service", t -> t.zeebeTaskType("external"))
            .endEvent()
            .done());

    testClient.createWorkflowInstance("process");

    // when
    testClient.completeJobOfType("external", OTHER_PAYLOAD);

    // then
    assertRecordPayload(WorkflowInstanceIntent.ELEMENT_COMPLETED, OTHER_DOCUMENT);
  }

  @Test
  public void shouldNotSeePayloadOfWorkflowInstanceBefore() {
    // given
    testClient.deploy(
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .serviceTask("service", t -> t.zeebeTaskType("external"))
            .endEvent()
            .done());

    final long firstWFInstanceKey = testClient.createWorkflowInstance("process", MSGPACK_PAYLOAD);
    final long secondWFInstanceKey = testClient.createWorkflowInstance("process");

    // when
    testClient.completeJobOfWorkflowInstance("external", firstWFInstanceKey, MSGPACK_PAYLOAD);
    testClient.completeJobOfWorkflowInstance(
        "external", secondWFInstanceKey, MsgPackUtil.asMsgPackArray("{'foo':'bar'}"));

    // then first event payload is expected as
    assertRecordPayload(
        testClient.receiveFirstWorkflowInstanceEvent(
            firstWFInstanceKey, WorkflowInstanceIntent.ELEMENT_COMPLETED),
        JSON_DOCUMENT);

    // and second event payload is expected as
    assertRecordPayload(
        testClient.receiveFirstWorkflowInstanceEvent(
            secondWFInstanceKey, WorkflowInstanceIntent.ELEMENT_COMPLETED),
        "{'foo':'bar'}");
  }

  @Test
  public void shouldNotSeePayloadOfWorkflowInstanceBeforeOnOutputMapping() {
    // given
    testClient.deploy(
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .serviceTask(
                "service", t -> t.zeebeTaskType("external").zeebeOutput("$", "$.taskPayload"))
            .endEvent()
            .done());

    final long firstWFInstanceKey = testClient.createWorkflowInstance("process", MSGPACK_PAYLOAD);
    final long secondWFInstanceKey =
        testClient.createWorkflowInstance(
            "process", MsgPackUtil.asMsgPackArray("{'otherPayload':'value'}"));

    // when
    testClient.completeJobOfWorkflowInstance("external", firstWFInstanceKey, MSGPACK_PAYLOAD);
    testClient.completeJobOfWorkflowInstance(
        "external", secondWFInstanceKey, MsgPackUtil.asMsgPackArray("{'foo':'bar'}"));

    // then first event payload is expected as
    assertRecordPayload(
        testClient.receiveFirstWorkflowInstanceEvent(
            firstWFInstanceKey, WorkflowInstanceIntent.ELEMENT_COMPLETED),
        "{'string':'value', 'jsonObject':{'testAttr':'test'},'taskPayload':{'string':'value', 'jsonObject':{'testAttr':'test'}}}");

    // and second event payload is expected as
    assertRecordPayload(
        testClient.receiveFirstWorkflowInstanceEvent(
            secondWFInstanceKey, WorkflowInstanceIntent.ELEMENT_COMPLETED),
        "{'otherPayload':'value','taskPayload':{'foo':'bar'}}");
  }

  @Test
  public void shouldUseDefaultOutputMappingIfOnlyInputMappingSpecified() {
    // given
    final Map<String, String> inputMapping = new HashMap<>();
    inputMapping.put(NODE_ROOT_PATH, NODE_ROOT_PATH);
    testClient.deploy(
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .serviceTask(
                "service",
                t -> t.zeebeTaskType("external").zeebeInput(NODE_ROOT_PATH, NODE_ROOT_PATH))
            .endEvent()
            .done());

    testClient.createWorkflowInstance("process", MSGPACK_PAYLOAD);

    // when
    testClient.completeJobOfType("external", OTHER_PAYLOAD);

    // then
    assertRecordPayload(WorkflowInstanceIntent.ELEMENT_COMPLETED, MERGED_OTHER_WITH_JSON_DOCUMENT);
  }

  @Test
  public void shouldUseWFPayloadIfCompleteWithNoPayload() {
    // given
    testClient.deploy(
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .serviceTask("service", t -> t.zeebeTaskType("external"))
            .endEvent()
            .done());

    testClient.createWorkflowInstance("process", MSGPACK_PAYLOAD);

    // when
    testClient.completeJobOfType("external");

    // then
    assertRecordPayload(WorkflowInstanceIntent.ELEMENT_COMPLETED, JSON_DOCUMENT);
  }

  @Test
  public void shouldUseOutputMappingToAddObjectsToWorkflowPayload() {
    // given
    testClient.deploy(
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .serviceTask(
                "service",
                t ->
                    t.zeebeTaskType("external")
                        .zeebeOutput(NODE_STRING_PATH, "$.newFoo")
                        .zeebeOutput(NODE_JSON_OBJECT_PATH, "$.newObj"))
            .endEvent()
            .done());

    testClient.createWorkflowInstance("process", MSGPACK_PAYLOAD);

    // when
    testClient.completeJobOfType("external", MSGPACK_PAYLOAD);
    assertRecordPayload(
        WorkflowInstanceIntent.ELEMENT_COMPLETED,
        "{'newFoo':'value', 'newObj':{'testAttr':'test'},"
            + " 'string':'value', 'jsonObject':{'testAttr':'test'}}");
  }

  @Test
  public void shouldUseInOutMapping() {
    // given
    testClient.deploy(
        Bpmn.createExecutableProcess("process")
            .startEvent()
            .serviceTask(
                "service",
                t ->
                    t.zeebeTaskType("external")
                        .zeebeInput(NODE_JSON_OBJECT_PATH, NODE_ROOT_PATH)
                        .zeebeOutput("$.testAttr", "$.result"))
            .endEvent()
            .done());

    // when
    testClient.createWorkflowInstance("process", MSGPACK_PAYLOAD);

    // then payload is expected as
    assertRecordPayload(JobIntent.CREATE, "{'testAttr':'test'}");

    // when
    testClient.completeJobOfType("external", MsgPackUtil.asMsgPackArray("{'testAttr':123}"));

    // then
    assertRecordPayload(
        WorkflowInstanceIntent.ELEMENT_COMPLETED,
        "{'string':'value', 'jsonObject':{'testAttr':'test'}, 'result':123}");
  }

  private void assertRecordPayload(JobIntent instanceIntent, String mergedOtherWithJsonDocument) {
    final Record activityCompletedEvent = testClient.receiveFirstJobEvent(instanceIntent);
    JobAssert.assertJobPayload(activityCompletedEvent, mergedOtherWithJsonDocument);
  }

  private void assertRecordPayload(
      WorkflowInstanceIntent instanceIntent, String mergedOtherWithJsonDocument) {
    final Record activityCompletedEvent =
        testClient.receiveFirstWorkflowInstanceEvent(instanceIntent);
    WorkflowAssert.assertWorkflowInstancePayload(
        activityCompletedEvent, mergedOtherWithJsonDocument);
  }

  private void assertRecordPayload(
      Record<WorkflowInstanceRecordValue> event, String mergedOtherWithJsonDocument) {
    WorkflowAssert.assertWorkflowInstancePayload(event, mergedOtherWithJsonDocument);
  }
}
