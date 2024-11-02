package spendreport.detailed;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import spendreport.detailed.model.DetailedAlert;
import spendreport.detailed.model.DetailedTransaction;


@Slf4j
public class DetailedFraudDetector extends KeyedProcessFunction<Long, DetailedTransaction, DetailedAlert> {

    private static final long serialVersionUID = 1L;

    private static final double SMALL_AMOUNT = 50.00;
    private static final double LARGE_AMOUNT = 500.00;
    private static final long ONE_MINUTE = 60 * 1000L;

    private transient ValueState<Boolean> flagState;
    private transient ValueState<Long> timerState;
    private transient ValueState<String> zipCodeState;

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Long, DetailedTransaction, DetailedAlert>.OnTimerContext ctx, Collector<DetailedAlert> out) throws Exception {
        timerState.clear();
        flagState.clear();
        zipCodeState.clear();
    }

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>("flag", Types.BOOLEAN);
        flagState = getRuntimeContext().getState(flagDescriptor);

        ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>("timer-state", Types.LONG);
        timerState = getRuntimeContext().getState(timerDescriptor);

        ValueStateDescriptor<String> zipCodeDescriptor = new ValueStateDescriptor<>("zip-code", Types.STRING);
        zipCodeState = getRuntimeContext().getState(zipCodeDescriptor);
    }

    @Override
    public void processElement(DetailedTransaction detailedTransaction, Context context, Collector<DetailedAlert> collector) throws Exception {

        Boolean lastTransactionWasSmall = flagState.value();

        if (lastTransactionWasSmall != null) {
            if (isTransactionFaulty(detailedTransaction)) {

                DetailedAlert alert = new DetailedAlert();
                long accountId = detailedTransaction.getAccountId();
                alert.setId(accountId);
                String zipCode = detailedTransaction.getZipCode();
                alert.setMessage(String.format("Large transaction detected for account %d of amount %.2f at zip code %s"
                        , accountId, detailedTransaction.getAmount(), zipCode));
                collector.collect(alert);
            }
            cleanUp(context);
        }

        if (detailedTransaction.getAmount() < SMALL_AMOUNT) {
            flagState.update(true);

            long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
            context.timerService().registerProcessingTimeTimer(timer);
            timerState.update(timer);
        }

        // Always update the zip code state
        zipCodeState.update(detailedTransaction.getZipCode());
    }

    @SneakyThrows
    private boolean isTransactionFaulty(DetailedTransaction transaction) {

        // The transaction is faulty if the amount is greater than the large amount and the zip code is the same as the state
        return transaction.getAmount() > LARGE_AMOUNT && transaction.getZipCode().equals(zipCodeState.value());
    }

    /**
     * Utility method to clean up the state
     *
     * @param ctx Context
     * @throws Exception
     */
    private void cleanUp(Context ctx) throws Exception { //NOSONAR
        Long timer = timerState.value();
        ctx.timerService().deleteEventTimeTimer(timer);
        timerState.clear();
        flagState.clear();
    }
}
