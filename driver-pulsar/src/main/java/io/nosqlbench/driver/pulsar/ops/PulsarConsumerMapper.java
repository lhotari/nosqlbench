package io.nosqlbench.driver.pulsar.ops;

import io.nosqlbench.driver.pulsar.PulsarActivity;
import io.nosqlbench.driver.pulsar.PulsarSpace;
import io.nosqlbench.engine.api.templating.CommandTemplate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.transaction.Transaction;

import java.util.function.LongFunction;
import java.util.function.Supplier;

/**
 * This maps a set of specifier functions to a pulsar operation. The pulsar operation contains
 * enough state to define a pulsar operation such that it can be executed, measured, and possibly
 * retried if needed.
 *
 * This function doesn't act *as* the operation. It merely maps the construction logic into
 * a simple functional type, given the component functions.
 *
 * For additional parameterization, the command template is also provided.
 */
public class PulsarConsumerMapper extends PulsarTransactOpMapper {

    private final static Logger logger = LogManager.getLogger(PulsarProducerMapper.class);

    private final LongFunction<Consumer<?>> consumerFunc;
    private final LongFunction<Boolean> topicMsgDedupFunc;
    private final LongFunction<String> subscriptionTypeFunc;
    private final boolean e2eMsProc;

    // Used for message loss checking
    private long prevMsgSeqId = -1;

    // Used for early quiting when there are message loss
    // Otherwise, sync API may unblock unnecessarily
    private long totalMsgLossCnt = 0;
    private long maxMsgSeqToExpect = -1;

    public PulsarConsumerMapper(CommandTemplate cmdTpl,
                                PulsarSpace clientSpace,
                                PulsarActivity pulsarActivity,
                                LongFunction<Boolean> asyncApiFunc,
                                LongFunction<Boolean> useTransactionFunc,
                                LongFunction<Boolean> seqTrackingFunc,
                                LongFunction<Supplier<Transaction>> transactionSupplierFunc,
                                LongFunction<Boolean> topicMsgDedupFunc,
                                LongFunction<Consumer<?>> consumerFunc,
                                LongFunction<String> subscriptionTypeFunc,
                                boolean e2eMsgProc) {
        super(cmdTpl, clientSpace, pulsarActivity, asyncApiFunc, useTransactionFunc, seqTrackingFunc, transactionSupplierFunc);
        this.consumerFunc = consumerFunc;
        this.topicMsgDedupFunc = topicMsgDedupFunc;
        this.subscriptionTypeFunc = subscriptionTypeFunc;
        this.e2eMsProc = e2eMsgProc;
    }

    public long getPrevMsgSeqId() { return prevMsgSeqId; }
    public void setPrevMsgSeqId(long prevMsgSeqId) { this.prevMsgSeqId = prevMsgSeqId; }

    public long getTotalMsgLossCnt() { return totalMsgLossCnt; }
    public void setTotalMsgLossCnt(long totalMsgLossCnt) { this.totalMsgLossCnt = totalMsgLossCnt; }

    public long getMaxMsgSeqToExpect() { return maxMsgSeqToExpect; }
    public void setMaxMsgSeqToExpect(long maxMsgSeqToExpect) { this.maxMsgSeqToExpect = maxMsgSeqToExpect; }

    @Override
    public PulsarOp apply(long value) {
        boolean seqTracking = seqTrackingFunc.apply(value);
        if ( seqTracking && (maxMsgSeqToExpect != -1) ) {
             if ( (value + totalMsgLossCnt) > maxMsgSeqToExpect) {
                 return new PulsarConumerEmptyOp(pulsarActivity);
             }
        }

        Consumer<?> consumer = consumerFunc.apply(value);
        boolean asyncApi = asyncApiFunc.apply(value);
        boolean useTransaction = useTransactionFunc.apply(value);
        Supplier<Transaction> transactionSupplier = transactionSupplierFunc.apply(value);
        boolean topicMsgDedup = topicMsgDedupFunc.apply(value);
        String subscriptionType = subscriptionTypeFunc.apply(value);

        return new PulsarConsumerOp(
            this,
            pulsarActivity,
            asyncApi,
            useTransaction,
            seqTracking,
            transactionSupplier,
            topicMsgDedup,
            consumer,
            subscriptionType,
            clientSpace.getPulsarSchema(),
            clientSpace.getPulsarClientConf().getConsumerTimeoutSeconds(),
            value,
            e2eMsProc);
    }
}
