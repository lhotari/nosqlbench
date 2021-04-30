package io.nosqlbench.driver.pularjms;

import com.codahale.metrics.Timer;
import io.nosqlbench.driver.pularjms.ops.PulsarJmsOp;
import io.nosqlbench.engine.api.activityapi.core.SyncAction;
import io.nosqlbench.engine.api.activityapi.errorhandling.modular.ErrorDetail;
import io.nosqlbench.engine.api.activityapi.planning.OpSequence;
import io.nosqlbench.engine.api.activityimpl.ActivityDef;
import io.nosqlbench.engine.api.activityimpl.OpDispenser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.function.LongFunction;

public class PulsarJmsAction implements SyncAction {

    private final static Logger logger = LogManager.getLogger(PulsarJmsAction.class);

    private final ActivityDef activityDef;
    private final int slot;

    private final PulsarJmsActivity activity;
    private OpSequence<OpDispenser<PulsarJmsOp>> sequencer;

    int maxTries = 1;

    public PulsarJmsAction(ActivityDef activityDef, int slot, PulsarJmsActivity activity) {
        this.activityDef = activityDef;
        this.slot = slot;
        this.activity = activity;
        this.maxTries = activity.getActivityDef().getParams().getOptionalInteger("maxtries").orElse(10);
    }

    @Override
    public void init() {
        this.sequencer = activity.getSequencer();
    }

    @Override
    public int runCycle(long cycle) {
        // let's fail the action if some async operation failed
        activity.failOnAsyncOperationFailure();

        long start = System.nanoTime();

        PulsarJmsOp pulsarJmsOp;
        try (Timer.Context ctx = activity.getBindTimer().time()) {
            LongFunction<PulsarJmsOp> readyPulsarJmsOp = sequencer.get(cycle);
            pulsarJmsOp = readyPulsarJmsOp.apply(cycle);
        } catch (Exception bindException) {
            // if diagnostic mode ...
            activity.getErrorhandler().handleError(bindException, cycle, 0);
            throw new RuntimeException(
                "while binding request in cycle " + cycle + ": " + bindException.getMessage(), bindException
            );
        }

        for (int i = 0; i < maxTries; i++) {
            Timer.Context ctx = activity.getExecuteTimer().time();
            try {
                // it is up to the pulsarOp to call Context#close when the activity is executed
                // this allows us to track time for async operations
                pulsarJmsOp.run(ctx::close);
                break;
            } catch (RuntimeException err) {
                ErrorDetail errorDetail = activity
                    .getErrorhandler()
                    .handleError(err, cycle, System.nanoTime() - start);
                if (!errorDetail.isRetryable()) {
                    break;
                }
            }
        }

        return 0;
    }
}
