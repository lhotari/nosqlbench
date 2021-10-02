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

    private long curMsgSeqId;
    private long prevMsgSeqId;

    private final Counter bytesCounter;
    private final Histogram messageSizeHistogram;
    private final Timer transactionCommitTimer;

    // keep track of end-to-end message latency
    private final Histogram e2eMsgProcLatencyHistogram;

    public PulsarConsumerOp(
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

        this.curMsgSeqId = 0;
        this.prevMsgSeqId = (curCycleNum - 1);

        this.bytesCounter = pulsarActivity.getBytesCounter();
        this.messageSizeHistogram = pulsarActivity.getMessageSizeHistogram();
        this.transactionCommitTimer = pulsarActivity.getCommitTransactionTimer();

        this.e2eMsgProcLatencyHistogram = pulsarActivity.getE2eMsgProcLatencyHistogram();
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

                        logger.debug("Sync message received: msg-key={}; msg-properties={}; msg-payload={}",
                            message.getKey(),
                            message.getProperties(),
                            avroGenericRecord.toString());
                    }
                    else {
                        logger.debug("Sync message received: msg-key={}; msg-properties={}; msg-payload={}",
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

                // keep track of message ordering and message loss
                String msgSeqIdStr = message.getProperties().get(PulsarActivityUtil.MSG_SEQUENCE_ID);
                if ( (seqTracking) && !StringUtils.isBlank(msgSeqIdStr) ) {
                    curMsgSeqId = Long.parseLong(msgSeqIdStr);

                    if ( prevMsgSeqId > -1) {
                        // normal case: message sequence id is monotonically increasing by 1
                        if ((curMsgSeqId - prevMsgSeqId) != 1) {
                            // abnormal case: out of ordering
                            // - for any subscription type, this check should always hold
                            if (curMsgSeqId < prevMsgSeqId) {
                                throw new PulsarMsgOutOfOrderException(
                                    false, curCycleNum, curMsgSeqId, prevMsgSeqId);
                            }
                            // - this sequence based message loss and message duplicate check can't be used for
                            //   "Shared" subscription (ignore this check)
                            // - TODO: for Key_Shared subscription type, this logic needs to be improved on
                            //         per-key basis
                            else {
                                if ( !StringUtils.equalsAnyIgnoreCase(subscriptionType,
                                    PulsarActivityUtil.SUBSCRIPTION_TYPE.Shared.label,
                                    PulsarActivityUtil.SUBSCRIPTION_TYPE.Key_Shared.label)) {
                                    // abnormal case: message loss
                                    if ((curMsgSeqId - prevMsgSeqId) > 1) {
                                        throw new PulsarMsgLossException(
                                            false, curCycleNum, curMsgSeqId, prevMsgSeqId);
                                    } else if (topicMsgDedup && (curMsgSeqId == prevMsgSeqId)) {
                                        throw new PulsarMsgDuplicateException(
                                            false, curCycleNum, curMsgSeqId, prevMsgSeqId);
                                    }
                                }
                            }
                        }
                    }
                }

                int messageSize = message.getData().length;
                bytesCounter.inc(messageSize);
                messageSizeHistogram.update(messageSize);

                if (useTransaction) {
                    consumer.acknowledgeAsync(message.getMessageId(), transaction).get();

                    // little problem: here we are counting the "commit" time
                    // inside the overall time spent for the execution of the consume operation
                    // we should refactor this operation as for PulsarProducerOp, and use the passed callback
                    // to track with precision the time spent for the operation and for the commit
                    try (Timer.Context ctx = transactionCommitTimer.time()) {
                        transaction.commit().get();
                    }
                }
                else {
                    consumer.acknowledge(message.getMessageId());
                }

            }
            catch (Exception e) {
                logger.error(
                    "Sync message receiving failed - timeout value: {} seconds ", timeoutSeconds);
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

                            logger.debug("Async message received: msg-key={}; msg-properties={}; msg-payload={})",
                                message.getKey(),
                                message.getProperties(),
                                avroGenericRecord.toString());
                        }
                        else {
                            logger.debug("Async message received: msg-key={}; msg-properties={}; msg-payload={})",
                                message.getKey(),
                                message.getProperties(),
                                new String(message.getData()));
                        }
                    }

                    long e2eMsgLatency = System.currentTimeMillis() - message.getPublishTime();
                    if (e2eMsgProc) {
                        e2eMsgProcLatencyHistogram.update(e2eMsgLatency);
                    }

                    // keep track of message ordering, message loss, and message duplication
                    String msgSeqIdStr = message.getProperties().get(PulsarActivityUtil.MSG_SEQUENCE_ID);
                    if ( (seqTracking) && !StringUtils.isBlank(msgSeqIdStr) ) {
                        curMsgSeqId = Long.parseLong(msgSeqIdStr);

                        if (prevMsgSeqId > -1) {
                            // normal case: message sequence id is monotonically increasing by 1
                            if ((curMsgSeqId - prevMsgSeqId) != 1) {
                                // abnormal case: out of ordering
                                // - for any subscription type, this check should always hold
                                if (curMsgSeqId < prevMsgSeqId) {
                                    throw new PulsarMsgOutOfOrderException(
                                        false, curCycleNum, curMsgSeqId, prevMsgSeqId);
                                }
                                // - this sequence based message loss and message duplicate check can't be used for
                                //   "Shared" subscription (ignore this check)
                                // - TODO: for Key_Shared subscription type, this logic needs to be improved on
                                //         per-key basis
                                else {
                                    if ( !StringUtils.equalsAnyIgnoreCase(subscriptionType,
                                        PulsarActivityUtil.SUBSCRIPTION_TYPE.Shared.label,
                                        PulsarActivityUtil.SUBSCRIPTION_TYPE.Key_Shared.label)) {
                                        // abnormal case: message loss
                                        if ((curMsgSeqId - prevMsgSeqId) > 1) {
                                            throw new PulsarMsgLossException(
                                                false, curCycleNum, curMsgSeqId, prevMsgSeqId);
                                        } else if (topicMsgDedup && (curMsgSeqId == prevMsgSeqId)) {
                                            throw new PulsarMsgDuplicateException(
                                                false, curCycleNum, curMsgSeqId, prevMsgSeqId);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    if (useTransaction) {
                        consumer.acknowledgeAsync(message.getMessageId(), transaction);
                    }
                    else {
                        consumer.acknowledgeAsync(message);
                    }

                    timeTracker.run();
                }).exceptionally(ex -> {
                    pulsarActivity.asyncOperationFailed(ex);
                    return null;
                });
            }
            catch (Exception e) {
                throw new PulsarDriverUnexpectedException("Async message receiving failed");
            }
        }
    }
}
