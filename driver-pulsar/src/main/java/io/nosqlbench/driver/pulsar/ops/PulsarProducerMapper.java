package io.nosqlbench.driver.pulsar.ops;

import io.nosqlbench.driver.pulsar.PulsarActivity;
import io.nosqlbench.driver.pulsar.PulsarSpace;
import io.nosqlbench.driver.pulsar.util.PulsarActivityUtil;
import io.nosqlbench.engine.api.templating.CommandTemplate;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.transaction.Transaction;

import java.util.HashMap;
import java.util.Map;
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
public class PulsarProducerMapper extends PulsarTransactOpMapper {

    private final static Logger logger = LogManager.getLogger(PulsarProducerMapper.class);

    private final LongFunction<Producer<?>> producerFunc;
    private final LongFunction<String> seqErrSimuTypeFunc;
    private final LongFunction<String> keyFunc;
    private final LongFunction<String> propFunc;
    private final LongFunction<String> payloadFunc;

    private final long totalCycleCount;

    public PulsarProducerMapper(CommandTemplate cmdTpl,
                                PulsarSpace clientSpace,
                                PulsarActivity pulsarActivity,
                                LongFunction<Boolean> asyncApiFunc,
                                LongFunction<Boolean> useTransactionFunc,
                                LongFunction<Boolean> seqTrackingFunc,
                                LongFunction<Supplier<Transaction>> transactionSupplierFunc,
                                LongFunction<Producer<?>> producerFunc,
                                LongFunction<String> seqErrSimuTypeFunc,
                                LongFunction<String> keyFunc,
                                LongFunction<String> propFunc,
                                LongFunction<String> payloadFunc) {
        super(cmdTpl, clientSpace, pulsarActivity, asyncApiFunc, useTransactionFunc, seqTrackingFunc, transactionSupplierFunc);

        this.producerFunc = producerFunc;
        this.seqErrSimuTypeFunc = seqErrSimuTypeFunc;
        this.keyFunc = keyFunc;
        this.propFunc = propFunc;
        this.payloadFunc = payloadFunc;

        this.totalCycleCount = pulsarActivity.getActivityDef().getCycleCount();
    }

    @Override
    public PulsarOp apply(long value) {
        boolean asyncApi = asyncApiFunc.apply(value);
        boolean useTransaction = useTransactionFunc.apply(value);
        boolean seqTracking = seqTrackingFunc.apply(value);
        Supplier<Transaction> transactionSupplier = transactionSupplierFunc.apply(value);

        Producer<?> producer = producerFunc.apply(value);

        boolean lastMsg = (value == (totalCycleCount-1));

        // Simulate error 10% of the time, but always ignore
        // the last message
        float rndVal = RandomUtils.nextFloat(0, 1.0f);
        boolean simulationError = (!lastMsg) && ((rndVal >= 0) && (rndVal < 0.2f));

        String seqErrSimuTypesStr = seqErrSimuTypeFunc.apply(value);
        boolean simulateMsgOutofOrder = simulationError &&
            !StringUtils.isBlank(seqErrSimuTypesStr) &&
            StringUtils.containsIgnoreCase(seqErrSimuTypesStr, PulsarActivityUtil.SEQ_ERROR_SIMU_TYPE.OutOfOrder.label);
        boolean simulateMsgLoss = simulationError &&
            !StringUtils.isBlank(seqErrSimuTypesStr) &&
            StringUtils.containsIgnoreCase(seqErrSimuTypesStr, PulsarActivityUtil.SEQ_ERROR_SIMU_TYPE.MsgLoss.label);
        boolean simulateMsgDup = simulationError &&
            !StringUtils.isBlank(seqErrSimuTypesStr) &&
            StringUtils.containsIgnoreCase(seqErrSimuTypesStr, PulsarActivityUtil.SEQ_ERROR_SIMU_TYPE.MsgDup.label);

        String msgKey = keyFunc.apply(value);
        String msgPayload = payloadFunc.apply(value);

        // Check if msgPropJonStr is valid JSON string with a collection of key/value pairs
        // - if Yes, convert it to a map
        // - otherwise, log an error message and ignore message properties without throwing a runtime exception
        Map<String, String> msgProperties = new HashMap<>();
        String msgPropJsonStr = propFunc.apply(value);
        if (!StringUtils.isBlank(msgPropJsonStr)) {
            try {
                msgProperties = PulsarActivityUtil.convertJsonToMap(msgPropJsonStr);

            } catch (Exception e) {
                logger.error(
                    "Error parsing message property JSON string {}, ignore message properties!",
                    msgPropJsonStr);
            }
        }

        // Error simulation sequence:
        // - message loss > message out of order > message duplication
        if (!simulateMsgLoss) {
            // Set message sequence tracking property
            if (seqTracking) {
                msgProperties.put(PulsarActivityUtil.MSG_SEQUENCE_TGTMAX,
                    String.valueOf(pulsarActivity.getActivityDef().getCycleCount()-1));

                // normal case
                if (!simulateMsgOutofOrder && !simulateMsgDup) {
                    msgProperties.put(PulsarActivityUtil.MSG_SEQUENCE_ID, String.valueOf(value));
                }
                else {
                    // simulate message out of order
                    if (simulateMsgOutofOrder) {
                        int rndmOffset = 2;
                        msgProperties.put(PulsarActivityUtil.MSG_SEQUENCE_ID,
                            String.valueOf((value > rndmOffset) ? (value - rndmOffset) : value));
                    }
                    // simulate message duplication
                    else if (simulateMsgDup) {
                        msgProperties.put(PulsarActivityUtil.MSG_SEQUENCE_ID, String.valueOf(value - 1));
                    }
                }
            }

            return new PulsarProducerOp(
                pulsarActivity,
                asyncApi,
                useTransaction,
                transactionSupplier,
                producer,
                clientSpace.getPulsarSchema(),
                msgKey,
                msgProperties,
                msgPayload);
        }
        else {
            // Simulate message loss, but don't simulate the scenario where
            // only the last set of message are lost
            return new PulsarProducerEmptyOp(pulsarActivity);
        }
    }
}
