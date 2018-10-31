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

import static io.zeebe.logstreams.rocksdb.ZeebeStateConstants.STATE_BYTE_ORDER;
import static io.zeebe.util.StringUtil.getBytes;
import static io.zeebe.util.buffer.BufferUtil.contentsEqual;

import io.zeebe.broker.util.KeyStateController;
import io.zeebe.logstreams.rocksdb.ZbRocksDb;
import io.zeebe.logstreams.rocksdb.ZbRocksDb.IteratorControl;
import io.zeebe.logstreams.rocksdb.ZbWriteBatch;
import io.zeebe.protocol.impl.record.value.job.JobRecord;
import io.zeebe.util.buffer.BufferWriter;
import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

public class JobStateController extends KeyStateController {
  private static final byte[] STATES_COLUMN_FAMILY_NAME = getBytes("states");
  private static final byte[] DEADLINES_COLUMN_FAMILY_NAME = getBytes("deadlines");
  private static final byte[] ACTIVATABLE_COLUMN_FAMILY_NAME = getBytes("activatable");
  public static final byte[][] COLUMN_FAMILY_NAMES = {
    STATES_COLUMN_FAMILY_NAME, ACTIVATABLE_COLUMN_FAMILY_NAME, DEADLINES_COLUMN_FAMILY_NAME
  };

  private static final DirectBuffer NULL = new UnsafeBuffer(new byte[] {0});

  // key => job record value
  private ColumnFamilyHandle defaultColumnFamily;
  // key => job state
  private ColumnFamilyHandle statesColumnFamily;
  // type => [key]
  private ColumnFamilyHandle activatableColumnFamily;
  // timeout => key
  private ColumnFamilyHandle deadlinesColumnFamily;

  private MutableDirectBuffer keyBuffer;
  private MutableDirectBuffer valueBuffer;

  private ZbRocksDb db;

  public static List<byte[]> getColumnFamilyNames() {
    return Stream.of(COLUMN_FAMILY_NAMES).collect(Collectors.toList());
  }

  @Override
  public RocksDB open(final File dbDirectory, final boolean reopen) throws Exception {
    final List<byte[]> columnFamilyNames = getColumnFamilyNames();

    final RocksDB rocksDB = super.open(dbDirectory, reopen, columnFamilyNames);
    keyBuffer = new ExpandableArrayBuffer();
    valueBuffer = new ExpandableArrayBuffer();

    defaultColumnFamily = rocksDB.getDefaultColumnFamily();
    statesColumnFamily = getColumnFamilyHandle(STATES_COLUMN_FAMILY_NAME);
    activatableColumnFamily = getColumnFamilyHandle(ACTIVATABLE_COLUMN_FAMILY_NAME);
    deadlinesColumnFamily = getColumnFamilyHandle(DEADLINES_COLUMN_FAMILY_NAME);

    return rocksDB;
  }

  @Override
  protected RocksDB openDb(DBOptions dbOptions) throws RocksDBException {
    db =
        ZbRocksDb.open(
            dbOptions, dbDirectory.getAbsolutePath(), columnFamilyDescriptors, columnFamilyHandles);
    return db;
  }

  public void create(final long key, final JobRecord record) {
    final DirectBuffer type = record.getType();
    DirectBuffer keyBuffer;
    DirectBuffer valueBuffer;

    try (WriteOptions options = new WriteOptions();
        ZbWriteBatch batch = new ZbWriteBatch()) {
      keyBuffer = getDefaultKey(key);
      valueBuffer = writeValue(record);
      batch.put(defaultColumnFamily, keyBuffer, valueBuffer);

      valueBuffer = writeStatesValue(State.ACTIVATABLE);
      batch.put(statesColumnFamily, keyBuffer, valueBuffer);

      keyBuffer = getActivatableKey(key, type);
      batch.put(activatableColumnFamily, keyBuffer, NULL);

      db.write(options, batch);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  public void activate(final long key, final JobRecord record) {
    final DirectBuffer type = record.getType();
    final long deadline = record.getDeadline();

    DirectBuffer keyBuffer;
    DirectBuffer valueBuffer;

    try (WriteOptions options = new WriteOptions();
        ZbWriteBatch batch = new ZbWriteBatch()) {
      keyBuffer = getDefaultKey(key);
      valueBuffer = writeValue(record);
      batch.put(defaultColumnFamily, keyBuffer, valueBuffer);

      valueBuffer = writeStatesValue(State.ACTIVATED);
      batch.put(statesColumnFamily, keyBuffer, valueBuffer);

      keyBuffer = getActivatableKey(key, type);
      batch.delete(activatableColumnFamily, keyBuffer);

      keyBuffer = getDeadlinesKey(key, deadline);
      batch.put(deadlinesColumnFamily, keyBuffer, NULL);

      db.write(options, batch);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  public void timeout(final long key, final JobRecord record) {
    final DirectBuffer type = record.getType();
    DirectBuffer valueBuffer;
    DirectBuffer keyBuffer;

    try (WriteOptions options = new WriteOptions();
        ZbWriteBatch batch = new ZbWriteBatch()) {

      keyBuffer = getDefaultKey(key);
      valueBuffer = writeValue(record);
      batch.put(defaultColumnFamily, keyBuffer, valueBuffer);

      valueBuffer = writeStatesValue(State.ACTIVATABLE);
      batch.put(statesColumnFamily, keyBuffer, valueBuffer);

      keyBuffer = getActivatableKey(key, type);
      batch.put(activatableColumnFamily, keyBuffer, NULL);

      keyBuffer = getDeadlinesKey(key, record.getDeadline());
      batch.delete(deadlinesColumnFamily, keyBuffer);

      db.write(options, batch);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  public void delete(long key, JobRecord record) {
    final DirectBuffer type = record.getType();
    DirectBuffer keyBuffer;

    try (WriteOptions options = new WriteOptions();
        ZbWriteBatch batch = new ZbWriteBatch()) {
      keyBuffer = getDefaultKey(key);
      batch.delete(defaultColumnFamily, keyBuffer);

      batch.delete(statesColumnFamily, keyBuffer);

      final DirectBuffer activatableKey = getActivatableKey(key, type);
      batch.delete(activatableColumnFamily, activatableKey);

      keyBuffer = getDeadlinesKey(key, record.getDeadline());
      batch.delete(deadlinesColumnFamily, keyBuffer);

      db.write(options, batch);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  public void fail(long key, JobRecord updatedValue) {
    final DirectBuffer type = updatedValue.getType();
    DirectBuffer valueBuffer;
    DirectBuffer keyBuffer;

    try (WriteOptions options = new WriteOptions();
        ZbWriteBatch batch = new ZbWriteBatch()) {
      keyBuffer = getDefaultKey(key);
      valueBuffer = writeValue(updatedValue);
      batch.put(defaultColumnFamily, keyBuffer, valueBuffer);

      final State newState = updatedValue.getRetries() > 0 ? State.ACTIVATABLE : State.FAILED;

      valueBuffer = writeStatesValue(newState);
      batch.put(statesColumnFamily, keyBuffer, valueBuffer);

      if (newState == State.ACTIVATABLE) {
        keyBuffer = getActivatableKey(key, type);
        batch.put(activatableColumnFamily, keyBuffer, NULL);
      }

      keyBuffer = getDeadlinesKey(key, updatedValue.getDeadline());
      batch.delete(deadlinesColumnFamily, keyBuffer);

      db.write(options, batch);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  public void resolve(long key, final JobRecord updatedValue) {
    final DirectBuffer type = updatedValue.getType();
    DirectBuffer valueBuffer;
    DirectBuffer keyBuffer;

    try (WriteOptions options = new WriteOptions();
        ZbWriteBatch batch = new ZbWriteBatch()) {
      keyBuffer = getDefaultKey(key);
      valueBuffer = writeValue(updatedValue);
      batch.put(defaultColumnFamily, keyBuffer, valueBuffer);

      valueBuffer = writeStatesValue(State.ACTIVATABLE);
      batch.put(statesColumnFamily, keyBuffer, valueBuffer);

      keyBuffer = getActivatableKey(key, type);
      batch.put(activatableColumnFamily, keyBuffer, NULL);

      db.write(options, batch);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  public void forEachTimedOutEntry(final long upperBound, final IteratorConsumer callback) {
    db.forEach(
        deadlinesColumnFamily,
        (e, c) -> {
          final long deadline = e.getKey().getLong(0, STATE_BYTE_ORDER);
          if (deadline < upperBound) {
            final DirectBuffer keyBuffer = new UnsafeBuffer(e.getKey(), Long.BYTES, Long.BYTES);
            callback.accept(keyBuffer.getLong(0, STATE_BYTE_ORDER), getJob(keyBuffer), c);
          } else {
            c.stop();
          }
        });
  }

  public boolean exists(long jobKey) {
    final DirectBuffer dbKey = getDefaultKey(jobKey);
    return db.exists(defaultColumnFamily, dbKey);
  }

  public boolean isInState(long key, State state) {
    final DirectBuffer keyBuffer = getDefaultKey(key);
    if (db.exists(statesColumnFamily, keyBuffer, valueBuffer)) {
      return valueBuffer.getByte(0) == state.value;
    } else {
      return false;
    }
  }

  public void forEachActivatableJobs(final DirectBuffer type, final IteratorConsumer callback) {
    final DirectBuffer prefix = getActivatablePrefix(type);

    // iterate by prefix, and since we're looking for exactly the type, once we find the first one,
    // it should iterate exactly over all those with that exact type, and once we hit a longer or
    // different one it should stop.
    db.forEachPrefixed(
        activatableColumnFamily,
        prefix,
        (e, c) -> {
          final DirectBuffer entryKey = e.getKey();
          final DirectBuffer typeBuffer =
              new UnsafeBuffer(entryKey, 0, entryKey.capacity() - Long.BYTES);
          if (contentsEqual(type, typeBuffer)) {
            final DirectBuffer keyBuffer =
                new UnsafeBuffer(entryKey, typeBuffer.capacity(), Long.BYTES);
            callback.accept(keyBuffer.getLong(0, STATE_BYTE_ORDER), getJob(keyBuffer), c);
          } else {
            c.stop();
          }
        });
  }

  public JobRecord getJob(final long key) {
    final DirectBuffer keyBuffer = getDefaultKey(key);
    return getJob(keyBuffer);
  }

  private JobRecord getJob(final DirectBuffer keyBuffer) {
    final int bytesRead = db.get(defaultColumnFamily, keyBuffer, valueBuffer);

    if (bytesRead == RocksDB.NOT_FOUND) {
      return null;
    }

    return readJob(valueBuffer, bytesRead);
  }

  private JobRecord readJob(final DirectBuffer buffer, final int length) {
    final JobRecord record = new JobRecord();
    record.wrap(buffer, 0, length);

    return record;
  }

  private UnsafeBuffer getDefaultKey(final long key) {
    keyBuffer.putLong(0, key, STATE_BYTE_ORDER);
    return new UnsafeBuffer(keyBuffer, 0, Long.BYTES);
  }

  private UnsafeBuffer getActivatableKey(final long key, final DirectBuffer type) {
    final int typeLength = type.capacity();
    keyBuffer.putBytes(0, type, 0, typeLength);
    keyBuffer.putLong(typeLength, key, STATE_BYTE_ORDER);

    return new UnsafeBuffer(keyBuffer, 0, typeLength + Long.BYTES);
  }

  private UnsafeBuffer getActivatablePrefix(final DirectBuffer type) {
    final int typeLength = type.capacity();
    keyBuffer.putBytes(0, type, 0, typeLength);

    return new UnsafeBuffer(keyBuffer, 0, typeLength);
  }

  private UnsafeBuffer getDeadlinesKey(final long key, final long deadline) {
    keyBuffer.putLong(0, deadline, STATE_BYTE_ORDER);
    keyBuffer.putLong(Long.BYTES, key, STATE_BYTE_ORDER);

    return new UnsafeBuffer(keyBuffer, 0, Long.BYTES * 2);
  }

  private UnsafeBuffer writeValue(final BufferWriter writer) {
    writer.write(valueBuffer, 0);
    return new UnsafeBuffer(valueBuffer, 0, writer.getLength());
  }

  private DirectBuffer writeStatesValue(final State state) {
    valueBuffer.putByte(0, state.value);
    return new UnsafeBuffer(valueBuffer, 0, 1);
  }

  public enum State {
    ACTIVATABLE((byte) 0),
    ACTIVATED((byte) 1),
    FAILED((byte) 2);

    byte value;

    State(byte value) {
      this.value = value;
    }
  }

  @FunctionalInterface
  public interface IteratorConsumer {
    void accept(final long key, final JobRecord record, final IteratorControl control);
  }
}
