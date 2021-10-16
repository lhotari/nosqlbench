package io.nosqlbench.driver.pulsar.ops;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.codahale.metrics.Counter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ReceivedMessageSequenceTrackerTest {
    Counter msgErrOutOfSeqCounter = new Counter();
    Counter msgErrDuplicateCounter = new Counter();
    Counter msgErrLossCounter = new Counter();
    ReceivedMessageSequenceTracker messageSequenceTracker = new ReceivedMessageSequenceTracker(msgErrOutOfSeqCounter, msgErrDuplicateCounter, msgErrLossCounter);

    @Test
    void shouldCountersBeZeroWhenSequenceDoesntContainGaps() {
        // when
        for (long l = 0; l < 100L; l++) {
            messageSequenceTracker.sequenceNumberReceived(l);
        }
        messageSequenceTracker.close();
        // then
        assertEquals(0, msgErrOutOfSeqCounter.getCount());
        assertEquals(0, msgErrDuplicateCounter.getCount());
        assertEquals(0, msgErrLossCounter.getCount());
    }

    @ParameterizedTest
    @ValueSource(longs = {10L, 11L, 19L, 20L, 21L, 100L})
    void shouldDetectMsgLoss(long totalMessages) {
        int messagesLost = 0;
        // when
        for (long l = 0; l < totalMessages; l++) {
            if (l % 2 == 1) {
                messagesLost++;
                continue;
            }
            messageSequenceTracker.sequenceNumberReceived(l);
        }
        if (totalMessages % 2 == 0) {
            messageSequenceTracker.sequenceNumberReceived(totalMessages);
        }
        messageSequenceTracker.close();
        // then
        assertEquals(0, msgErrOutOfSeqCounter.getCount());
        assertEquals(0, msgErrDuplicateCounter.getCount());
        assertEquals(messagesLost, msgErrLossCounter.getCount());
    }

    @ParameterizedTest
    @ValueSource(longs = {10L, 11L, 19L, 20L, 21L, 100L})
    void shouldDetectMsgDuplication(long totalMessages) {
        int messagesDuplicated = 0;
        // when
        for (long l = 0; l < totalMessages; l++) {
            if (l % 2 == 1) {
                messagesDuplicated++;
                messageSequenceTracker.sequenceNumberReceived(l);
            }
            messageSequenceTracker.sequenceNumberReceived(l);
        }
        if (totalMessages % 2 == 0) {
            messageSequenceTracker.sequenceNumberReceived(totalMessages);
        }
        if (totalMessages < 2 * ReceivedMessageSequenceTracker.MAX_TRACK_OUT_OF_ORDER_SEQUENCE_NUMBERS) {
            messageSequenceTracker.close();
        }

        // then
        assertEquals(0, msgErrOutOfSeqCounter.getCount());
        assertEquals(messagesDuplicated, msgErrDuplicateCounter.getCount());
        assertEquals(0, msgErrLossCounter.getCount());
    }

    @Test
    void shouldDetectSingleMessageOutOfSequence() {
        // when
        for (long l = 0; l < 10L; l++) {
            messageSequenceTracker.sequenceNumberReceived(l);
        }
        messageSequenceTracker.sequenceNumberReceived(10L);
        messageSequenceTracker.sequenceNumberReceived(12L);
        messageSequenceTracker.sequenceNumberReceived(11L);
        for (long l = 13L; l < 100L; l++) {
            messageSequenceTracker.sequenceNumberReceived(l);
        }

        // then
        assertEquals(1, msgErrOutOfSeqCounter.getCount());
        assertEquals(0, msgErrDuplicateCounter.getCount());
        assertEquals(0, msgErrLossCounter.getCount());
    }

    @Test
    void shouldDetectMultipleMessagesOutOfSequence() {
        // when
        for (long l = 0; l < 10L; l++) {
            messageSequenceTracker.sequenceNumberReceived(l);
        }
        messageSequenceTracker.sequenceNumberReceived(10L);
        messageSequenceTracker.sequenceNumberReceived(14L);
        messageSequenceTracker.sequenceNumberReceived(13L);
        messageSequenceTracker.sequenceNumberReceived(11L);
        messageSequenceTracker.sequenceNumberReceived(12L);
        for (long l = 15L; l < 100L; l++) {
            messageSequenceTracker.sequenceNumberReceived(l);
        }

        // then
        assertEquals(2, msgErrOutOfSeqCounter.getCount());
        assertEquals(0, msgErrDuplicateCounter.getCount());
        assertEquals(0, msgErrLossCounter.getCount());
    }

}
