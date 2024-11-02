package spendreport.detailed;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import spendreport.detailed.model.DetailedAlert;
import spendreport.detailed.model.DetailedTransaction;
import spendreport.detailed.sink.DetailedAlertSink;
import spendreport.detailed.source.DetailedTransactionSource;

public class DetailedFraudDetectorJob {

    @SneakyThrows
    public static void main(String[] args) {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set this flag to run bounded or not
        boolean bounded = false;

        DataStream<DetailedTransaction> transactions = environment
            .addSource(new DetailedTransactionSource(bounded))
            .name("transactions");

        DataStream<DetailedAlert> alerts = transactions
            .keyBy(DetailedTransaction::getAccountId)
            .process(new DetailedFraudDetector())
            .name("detailed-fraud-detector");

        alerts.addSink(new DetailedAlertSink())
            .name("send-detailed-alerts");

        environment.execute("Detailed Fraud Detector");
    }
}
