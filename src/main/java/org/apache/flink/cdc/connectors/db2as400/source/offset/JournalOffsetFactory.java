package org.apache.flink.cdc.connectors.db2as400.source.offset;

import static org.apache.flink.cdc.connectors.db2as400.source.offset.JournalOffset.OFFSET_KEY;
import static org.apache.flink.cdc.connectors.db2as400.source.offset.JournalOffset.PROCESSED_KEY;
import static org.apache.flink.cdc.connectors.db2as400.source.offset.JournalOffset.RECEIVER_KEY;
import static org.apache.flink.cdc.connectors.db2as400.source.offset.JournalOffset.RECEIVER_LIBRARY_KEY;
import static org.apache.flink.cdc.connectors.db2as400.source.offset.JournalOffset.TIMESTAMP_KEY;

import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Map;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;

public class JournalOffsetFactory extends OffsetFactory {

  @Override
  public Offset newOffset(Map<String, String> offsetMap) {
    if (offsetMap == null || offsetMap.isEmpty()) return createInitialOffset();
    BigInteger offset = new BigInteger(offsetMap.getOrDefault(OFFSET_KEY, "0"));
    String receiver = offsetMap.getOrDefault(RECEIVER_KEY, "");
    String receiverLibrary = offsetMap.getOrDefault(RECEIVER_LIBRARY_KEY, "");

    boolean processed = Boolean.parseBoolean(offsetMap.getOrDefault(PROCESSED_KEY, "true"));

    return new JournalOffset(
        new JournalProcessedPosition(
            offset.toString(),
            receiver,
            receiverLibrary,
            Instant.ofEpochSecond(Long.parseLong(offsetMap.getOrDefault(TIMESTAMP_KEY, "0"))),
            processed));
  }

  @Override
  public Offset newOffset(String filename, Long position) {
    throw new UnsupportedOperationException(
        "not supported create new Offset by filename and position.");
  }

  @Override
  public Offset newOffset(Long position) {
    throw new UnsupportedOperationException("not supported create new Offset by position.");
  }

  @Override
  public Offset createTimestampOffset(long timestampMillis) {
    throw new UnsupportedOperationException("not supported create new Offset by timestamp.");
  }

  @Override
  public Offset createInitialOffset() {
    return new JournalOffset(
        new JournalProcessedPosition("0", "receiver", "receiverLibrary", Instant.now(), false));
  }

  @Override
  public Offset createNoStoppingOffset() {
    return new JournalOffset(
        new JournalProcessedPosition(
            String.valueOf(Long.MAX_VALUE), "receiver", "receiverLibrary", Instant.now(), false));
  }
}
