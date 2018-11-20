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

import io.zeebe.broker.logstreams.processor.SideEffectProducer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public class SideEffectQueue implements SideEffectProducer, Consumer<SideEffectProducer> {
  private final List<SideEffectProducer> sideEffects = new ArrayList<>();

  public void clear() {
    sideEffects.clear();
  }

  @Override
  public boolean flush() {
    boolean flushed = true;
    final Iterator<SideEffectProducer> iterator = sideEffects.iterator();

    while (iterator.hasNext()) {
      if (iterator.next().flush()) {
        iterator.remove();
      } else {
        flushed = false;
      }
    }

    return flushed;
  }

  @Override
  public void accept(SideEffectProducer sideEffectProducer) {
    sideEffects.add(sideEffectProducer);
  }
}
