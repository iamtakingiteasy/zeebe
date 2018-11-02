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
package io.zeebe.broker.workflow.state.prototype;

import io.zeebe.broker.benchmarks.state.prototype.GetVariableBenchmark;
import io.zeebe.broker.benchmarks.state.prototype.SetVariableBenchmark;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class BenchmarkRunner {

  public static void main(String[] args) throws RunnerException {
    final Options opt =
        new OptionsBuilder()
            .include(".*" + GetVariableBenchmark.class.getSimpleName() + ".*")
            .include(".*" + SetVariableBenchmark.class.getSimpleName() + ".*")
            .forks(1)
            .build();
    new Runner(opt).run();
  }
}
