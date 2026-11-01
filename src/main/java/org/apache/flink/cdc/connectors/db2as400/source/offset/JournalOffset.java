package org.apache.flink.cdc.connectors.db2as400.source.offset;

import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;

/** IBM i Journal 位置偏移量实现 包装 Debezium 的 JournalProcessedPosition */
public class JournalOffset extends Offset {

  protected static final String OFFSET_KEY = "offset.event_sequence";
  protected static final String RECEIVER_KEY = "offset.receiver";
  protected static final String RECEIVER_LIBRARY_KEY = "offset.receiver_library";
  protected static final String TIMESTAMP_KEY = "offset.time";
  protected static final String PROCESSED_KEY = "offset.processed";

  public static final Offset NO_STOPPING_OFFSET = null;
  public static final Offset INITIAL_OFFSET = createInitialOffset();

  public JournalOffset(JournalProcessedPosition position) {
    if (this.offset == null) {
      this.offset = new HashMap<>();
    }
    this.offset.put(OFFSET_KEY, position.getOffset().toString());
    this.offset.put(RECEIVER_KEY, position.getReceiver().name());
    this.offset.put(RECEIVER_LIBRARY_KEY, position.getReceiver().library());
    this.offset.put(
        TIMESTAMP_KEY, String.valueOf(position.getTimeOfLastProcessed().getEpochSecond()));
    this.offset.put(PROCESSED_KEY, String.valueOf(position.processed()));
  }

  public JournalOffset(Map<String, String> offset) {
    if (this.offset == null) {
      this.offset = new HashMap<>();
    }
    this.offset.putAll(offset);
  }

  @Override
  public int compareTo(Offset other) {
    if (!(other instanceof JournalOffset)) {
      throw new IllegalArgumentException("Cannot compare different offset types");
    }
    JournalOffset that = (JournalOffset) other;
    String receiver = this.getOffset().get(RECEIVER_KEY);
    String thatReceiver = that.getOffset().get(RECEIVER_KEY);

    // 先比较 receiver
    int receiverCompare = receiver.compareTo(thatReceiver);
    if (receiverCompare != 0) {
      return receiverCompare;
    }
    String offset = this.getOffset().get(OFFSET_KEY);
    String thatOffset = this.getOffset().get(OFFSET_KEY);
    // 再比较 offset
    return offset.compareTo(thatOffset);
  }

  public static Offset createInitialOffset() {
    return new JournalOffset(
        new JournalProcessedPosition("0", "receiver", "receiverLibrary", Instant.now(), false));
  }
}
