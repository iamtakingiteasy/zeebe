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
package io.zeebe.client.api.commands;

import io.zeebe.client.api.response.ActivateJobsResponse;
import io.zeebe.client.api.response.ActivatedJob;
import java.time.Duration;

public interface ActivateJobsCommandStep1 {

  /**
   * Set the type of jobs to work on.
   *
   * @param jobType the type of jobs (e.g. "payment")
   * @return the builder for this command
   */
  ActivateJobsCommandStep2 jobType(String jobType);

  interface ActivateJobsCommandStep2 {

    /**
     * Set the maximal amount of jobs to activate. If less jobs are avaiable for activation the
     * command will return a list with fewer jobs.
     *
     * @param amount the maximal number of jobs to activate
     * @return the builder for this command
     */
    ActivateJobsCommandStep3 amount(int amount);
  }

  interface ActivateJobsCommandStep3 extends FinalCommandStep<ActivateJobsResponse> {

    /**
     * Set the time for how long a job is exclusively assigned for this subscription.
     *
     * <p>In this time, the job can not be assigned by other subscriptions to ensure that only one
     * subscription work on the job. When the time is over then the job can be assigned again by
     * this or other subscription if it's not completed yet.
     *
     * <p>If no timeout is set, then the default is used from the configuration.
     *
     * @param timeout the time in milliseconds
     * @return the builder for this command. Call {@link #send()} to complete the command and send
     *     it to the broker.
     */
    ActivateJobsCommandStep3 timeout(long timeout);

    /**
     * Set the time for how long a job is exclusively assigned for this subscription.
     *
     * <p>In this time, the job can not be assigned by other subscriptions to ensure that only one
     * subscription work on the job. When the time is over then the job can be assigned again by
     * this or other subscription if it's not completed yet.
     *
     * <p>If no time is set then the default is used from the configuration.
     *
     * @param timeout the time as duration (e.g. "Duration.ofMinutes(5)")
     * @return the builder for this command. Call {@link #send()} to complete the command and send
     *     it to the broker.
     */
    ActivateJobsCommandStep3 timeout(Duration timeout);

    /**
     * Set the name of the job worker.
     *
     * <p>This name is used to identify the worker which activated the jobs. Its main purpose is for
     * monitoring and auditing. Commands on activated jobs do not check the worker name, i.e.
     * complete or fail job.
     *
     * <p>If no name is set then the default is used from the configuration.
     *
     * @param workerName the name of the worker (e.g. "payment-service")
     * @return the builder for this command. Call {@link #send()} to complete the command and send
     *     it to the broker.
     */
    ActivateJobsCommandStep3 workerName(String workerName);

    /**
     * Select the workflow variables to fetch along with the job. Fetches all variables if no
     * variables are selected. The fetched variables can be accessed as a single JSON document via
     * {@link ActivatedJob#getPayload()}.
     *
     * @param variables names of variables to fetch
     * @return this builder. Call {@link #send()} to complete the command and send it to the broker.
     */
    ActivateJobsCommandStep3 variables(String... variables);
  }
}
