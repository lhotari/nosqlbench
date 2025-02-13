package io.nosqlbench.driver.pulsar.ops;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import io.nosqlbench.driver.pulsar.PulsarActivity;
import io.nosqlbench.driver.pulsar.exception.*;
import io.nosqlbench.driver.pulsar.util.AvroUtil;
import io.nosqlbench.driver.pulsar.util.PulsarActivityUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class PulsarConsumerOp implements PulsarOp {

    private final static Logger logger = LogManager.getLogger(PulsarConsumerOp.class);

    private final PulsarConsumerMapper consumerMapper;
    private final PulsarActivity pulsarActivity;

    private final boolean asyncPulsarOp;
    private final boolean useTransaction;
    private final boolean seqTracking;
    private final Supplier<Transaction> transactionSupplier;

    private final boolean topicMsgDedup;
    private final Consumer<?> consumer;
    private final String subscriptionType;
    private final Schema<?> pulsarSchema;
    private final int timeoutSeconds;
    private final boolean e2eMsgProc;
    private final long curCycleNum;

    private final Counter bytesCounter;
    private final Histogram messageSizeHistogram;
    private final Timer transactionCommitTimer;

    // keep track of end-to-end message latency
    private final Histogram e2eMsgProcLatencyHistogram;
    // message out-of-sequence error counter
    private final Counter msgErrOutOfSeqCounter;
    // message out-of-sequence error counter
    private final Counter msgErrDuplicateCounter;
    // message loss error counter
    private final Counter msgErrLossCounter;

    // Used for message error tracking
    private final boolean ignoreMsgLossCheck;
    private final boolean ignoreMsgDupCheck;

    public PulsarConsumerOp(
        PulsarConsumerMapper consumerMapper,
        PulsarActivity pulsarActivity,
        boolean asyncPulsarOp,
        boolean useTransaction,
        boolean seqTracking,
        Supplier<Transaction> transactionSupplier,
        boolean topicMsgDedup,
        Consumer<?> consumer,
        String subscriptionType,
        Schema<?> schema,
        int timeoutSeconds,
        long curCycleNum,
        boolean e2eMsgProc)
    {
        this.consumerMapper = consumerMapper;
        this.pulsarActivity = pulsarActivity;

        this.asyncPulsarOp = asyncPulsarOp;
        this.useTransaction = useTransaction;
        this.seqTracking = seqTracking;
        this.transactionSupplier = transactionSupplier;

        this.topicMsgDedup = topicMsgDedup;
        this.consumer = consumer;
        this.subscriptionType = subscriptionType;
        this.pulsarSchema = schema;
        this.timeoutSeconds = timeoutSeconds;
        this.curCycleNum = curCycleNum;
        this.e2eMsgProc = e2eMsgProc;

        this.bytesCounter = pulsarActivity.getBytesCounter();
        this.messageSizeHistogram = pulsarActivity.getMessageSizeHistogram();
        this.transactionCommitTimer = pulsarActivity.getCommitTransactionTimer();

        this.e2eMsgProcLatencyHistogram = pulsarActivity.getE2eMsgProcLatencyHistogram();
        this.msgErrOutOfSeqCounter = pulsarActivity.getMsgErrOutOfSeqCounter();
        this.msgErrLossCounter = pulsarActivity.getMsgErrLossCounter();
        this.msgErrDuplicateCounter = pulsarActivity.getMsgErrDuplicateCounter();

        // When message deduplication configuration is not enable, ignore message
        // duplication check
        this.ignoreMsgDupCheck = !this.topicMsgDedup;

        // Limitations of the message sequence based check:
        // - For message out of sequence and message duplicate check, it works for
        //   all subscription types, including "Shared" and "Key_Shared"
        // - For message loss, it doesn't work for "Shared" and "Key_Shared"
        //   subscription types
        this.ignoreMsgLossCheck =
            StringUtils.equalsAnyIgnoreCase(this.subscriptionType,
                PulsarActivityUtil.SUBSCRIPTION_TYPE.Shared.label,
                PulsarActivityUtil.SUBSCRIPTION_TYPE.Key_Shared.label);
    }

    private void checkAndUpdateMessageErrorCounter(Message message) {
        long maxMsgSeqToExpect = consumerMapper.getMaxMsgSeqToExpect();
        if (maxMsgSeqToExpect == -1) {
            String msgSeqTgtMaxStr = message.getProperty(PulsarActivityUtil.MSG_SEQUENCE_TGTMAX);
            if (!StringUtils.isBlank(msgSeqTgtMaxStr)) {
                consumerMapper.setMaxMsgSeqToExpect(Long.valueOf(msgSeqTgtMaxStr));
            }
        }

        String msgSeqIdStr = message.getProperty(PulsarActivityUtil.MSG_SEQUENCE_ID);

        if ( !StringUtils.isBlank(msgSeqIdStr) ) {
            long prevMsgSeqId = consumerMapper.getPrevMsgSeqId();
            long curMsgSeqId = Long.parseLong(msgSeqIdStr);

            // Skip out-of-sequence check on the first received message
            // - This is because out-of-sequence check requires at least 2
            //   received messages for comparison
            if ( (prevMsgSeqId != -1) && (curMsgSeqId < prevMsgSeqId) ) {
                msgErrOutOfSeqCounter.inc();
            }

            // Similarly, when message duplicate check is needed, we also
            // skip the first received message.
            if ( !ignoreMsgDupCheck && (prevMsgSeqId != -1) && (curMsgSeqId == prevMsgSeqId) ) {
                msgErrDuplicateCounter.inc();
            }

            // Note that message loss could be happened anywhere, E.g.
            // - published messages: 0,1,2,3,4,5
            // - message loss scenario:
            //   * scenario 1: first set of messages are lost - received 2,3,4
            //   * scenario 2: messages in the middle are lost - received 0,1,3,4
            //   * scenario 3: last set of messages are lost - received 0,1,2
            if ( !ignoreMsgLossCheck ) {
                // This check covers message loss scenarios 1 and 2
                if ( (curMsgSeqId - prevMsgSeqId) > 1 ){
                    // there could be multiple published messages lost between
                    // 2 received messages
                    long msgLostCnt = (curMsgSeqId - prevMsgSeqId) - 1;
                    msgErrLossCounter.inc(msgLostCnt);
                    consumerMapper.setTotalMsgLossCnt(consumerMapper.getTotalMsgLossCnt() + msgLostCnt);
                }

                // TODO: how can we detect message loss scenario 3?
            }

            prevMsgSeqId = curMsgSeqId;
            consumerMapper.setPrevMsgSeqId(prevMsgSeqId);
        }
    }

    @Override
    public void run(Runnable timeTracker) {

        final Transaction transaction;
        if (useTransaction) {
            // if you are in a transaction you cannot set the schema per-message
            transaction = transactionSupplier.get();
        }
        else {
            transaction = null;
        }

        if (!asyncPulsarOp) {
            Message<?> message;

            try {
                if (timeoutSeconds <= 0) {
                    // wait forever
                    message = consumer.receive();
                }
                else {
                    // we cannot use Consumer#receive(timeout, timeunit) due to
                    // https://github.com/apache/pulsar/issues/9921
                    message = consumer
                        .receiveAsync()
                        .get(timeoutSeconds, TimeUnit.SECONDS);
                }

                if (logger.isDebugEnabled()) {
                    SchemaType schemaType = pulsarSchema.getSchemaInfo().getType();

                    if (PulsarActivityUtil.isAvroSchemaTypeStr(schemaType.name())) {
                        String avroDefStr = pulsarSchema.getSchemaInfo().getSchemaDefinition();
                        org.apache.avro.Schema avroSchema =
                            AvroUtil.GetSchema_ApacheAvro(avroDefStr);
                        org.apache.avro.generic.GenericRecord avroGenericRecord =
                            AvroUtil.GetGenericRecord_ApacheAvro(avroSchema, message.getData());

                        logger.debug("({}) Sync message received: msg-key={}; msg-properties={}; msg-payload={}",
                            consumer.getConsumerName(),
                            message.getKey(),
                            message.getProperties(),
                            avroGenericRecord.toString());
                    }
                    else {
                        logger.debug("({}) Sync message received: msg-key={}; msg-properties={}; msg-payload={}",
                            consumer.getConsumerName(),
                            message.getKey(),
                            message.getProperties(),
                            new String(message.getData()));
                    }
                }

                // keep track end-to-end message processing latency
                long e2eMsgLatency = System.currentTimeMillis() - message.getPublishTime();
                if (e2eMsgProc) {
                    e2eMsgProcLatencyHistogram.update(e2eMsgLatency);
                }

                // keep track of message errors and update error counters
                if (seqTracking) checkAndUpdateMessageErrorCounter(message);

                int messageSize = message.getData().length;
                bytesCounter.inc(messageSize);
                messageSizeHistogram.update(messageSize);

                if (!useTransaction) {
                    consumer.acknowledge(message.getMessageId());
                }
                else {
                    consumer.acknowledgeAsync(message.getMessageId(), transaction).get();

                    // little problem: here we are counting the "commit" time
                    // inside the overall time spent for the execution of the consume operation
                    // we should refactor this operation as for PulsarProducerOp, and use the passed callback
                    // to track with precision the time spent for the operation and for the commit
                    try (Timer.Context ctx = transactionCommitTimer.time()) {
                        transaction.commit().get();
                    }
                }

            }
            catch (Exception e) {
                logger.error(
                    "Sync message receiving failed - timeout value: {} seconds ", timeoutSeconds);
                e.printStackTrace();
                throw new PulsarDriverUnexpectedException("" +
                    "Sync message receiving failed - timeout value: " + timeoutSeconds + " seconds ");
            }
        }
        else {
            try {
                CompletableFuture<? extends Message<?>> msgRecvFuture = consumer.receiveAsync();
                if (useTransaction) {
                    // add commit step
                    msgRecvFuture = msgRecvFuture.thenCompose(msg -> {
                            Timer.Context ctx = transactionCommitTimer.time();
                            return transaction
                                .commit()
                                .whenComplete((m,e) -> ctx.close())
                                .thenApply(v-> msg);
                        }
                    );
                }

                msgRecvFuture.whenComplete((message, error) -> {
                    int messageSize = message.getData().length;
                    bytesCounter.inc(messageSize);
                    messageSizeHistogram.update(messageSize);

                    if (logger.isDebugEnabled()) {
                        SchemaType schemaType = pulsarSchema.getSchemaInfo().getType();

                        if (PulsarActivityUtil.isAvroSchemaTypeStr(schemaType.name())) {
                            String avroDefStr = pulsarSchema.getSchemaInfo().getSchemaDefinition();
                            org.apache.avro.Schema avroSchema =
                                AvroUtil.GetSchema_ApacheAvro(avroDefStr);
                            org.apache.avro.generic.GenericRecord avroGenericRecord =
                                AvroUtil.GetGenericRecord_ApacheAvro(avroSchema, message.getData());

                            logger.debug("({}) Async message received: msg-key={}; msg-properties={}; msg-payload={})",
                                consumer.getConsumerName(),
                                message.getKey(),
                                message.getProperties(),
                                avroGenericRecord.toString());
                        }
                        else {
                            logger.debug("({}) Async message received: msg-key={}; msg-properties={}; msg-payload={})",
                                consumer.getConsumerName(),
                                message.getKey(),
                                message.getProperties(),
                                new String(message.getData()));
                        }
                    }

                    long e2eMsgLatency = System.currentTimeMillis() - message.getPublishTime();
                    if (e2eMsgProc) {
                        e2eMsgProcLatencyHistogram.update(e2eMsgLatency);
                    }

                    // keep track of message errors and update error counters
                    if (seqTracking) checkAndUpdateMessageErrorCounter(message);

                    if (!useTransaction) {
                        consumer.acknowledgeAsync(message);
                    }
                    else {
                        consumer.acknowledgeAsync(message.getMessageId(), transaction);
                    }

                    timeTracker.run();
                }).exceptionally(ex -> {
                    pulsarActivity.asyncOperationFailed(ex);
                    return null;
                });
            }
            catch (Exception e) {
                throw new PulsarDriverUnexpectedException(e);
            }
        }
    }
}
