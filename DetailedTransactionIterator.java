package spendreport.detailed.source;

import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.Setter;
import spendreport.detailed.model.DetailedTransaction;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;

/**
 * An iterator for generating detailed transaction events.
 * It is used as the source for transactions in the Flink pipeline.
 *
 * The iterator can be bounded (finite) or unbounded (infinite):
 * - Bounded: Stops returning elements once the list of transactions is exhausted.
 * - Unbounded: Continues returning elements in a round-robin fashion.
 */
@Getter
@Setter
final class DetailedTransactionIterator implements Iterator<DetailedTransaction>, Serializable {

    private static final long serialVersionUID = 1L;

    // Initial timestamp for the transactions, set to the current time
    private static final Timestamp INITIAL_TIMESTAMP = new Timestamp(Instant.now().toEpochMilli());

    // Define zip code constants to avoid repeated hardcoding
    private static final String ZIP_CODE_1 = "01003";
    private static final String ZIP_CODE_2 = "02115";
    private static final String ZIP_CODE_3 = "78712";

    /**
     * Creates an unbounded transaction iterator.
     * It will loop through the list of transactions indefinitely.
     *
     * @return DetailedTransactionIterator instance
     */
    static DetailedTransactionIterator unbounded() {
        return new DetailedTransactionIterator(false);
    }

    /**
     * Creates a bounded transaction iterator.
     * It will stop once all transactions in the list are returned.
     *
     * @return DetailedTransactionIterator instance
     */
    static DetailedTransactionIterator bounded() {
        return new DetailedTransactionIterator(true);
    }

    // Indicates whether the iterator is bounded or unbounded
    private final boolean bounded;

    // Timestamp of the last transaction returned, used to set the timestamp of the next transaction
    private long timestamp;

    // List of predefined detailed transactions
    private static List<DetailedTransaction> detailedTransactions = Lists.newArrayList();

    // Index to track the current transaction being returned
    private int runningIndex = 0;

    /**
     * Default constructor for creating an unbounded iterator.
     */
    public DetailedTransactionIterator() {
        this(false);
    }

    /**
     * Constructor that sets the iterator as bounded or unbounded.
     * Initializes the list of transactions and sets the initial timestamp.
     *
     * @param bounded - true for a bounded iterator, false for unbounded
     */
    private DetailedTransactionIterator(boolean bounded) {
        this.bounded = bounded;
        this.timestamp = INITIAL_TIMESTAMP.getTime();
        initData();
    }

    /**
     * Initializes a list of detailed transactions.
     * The list contains both normal and faulty patterns to simulate various scenarios.
     */
    private void initData() {
        detailedTransactions.addAll(
                Lists.newArrayList(
                        new DetailedTransaction(1, 0L, 7.25, ZIP_CODE_1),
                        new DetailedTransaction(2, 0L, 850.75, ZIP_CODE_2),
                        new DetailedTransaction(3, 0L, 9.50, ZIP_CODE_3),
                        new DetailedTransaction(4, 0L, 8.30, ZIP_CODE_1),   // Faulty pattern starts
                        new DetailedTransaction(4, 0L, 1000.00, ZIP_CODE_1), // Faulty pattern continues
                        new DetailedTransaction(5, 0L, 600.00, ZIP_CODE_2),
                        new DetailedTransaction(1, 0L, 6.00, ZIP_CODE_3),
                        new DetailedTransaction(3, 0L, 900.00, ZIP_CODE_3),
                        new DetailedTransaction(2, 0L, 5.50, ZIP_CODE_2),   // Faulty pattern starts
                        new DetailedTransaction(2, 0L, 950.00, ZIP_CODE_2), // Faulty pattern continues
                        new DetailedTransaction(1, 0L, 7.75, ZIP_CODE_1),
                        new DetailedTransaction(4, 0L, 650.00, ZIP_CODE_1),
                        new DetailedTransaction(5, 0L, 5.25, ZIP_CODE_2),   // Faulty pattern starts
                        new DetailedTransaction(5, 0L, 700.00, ZIP_CODE_2), // Faulty pattern continues
                        new DetailedTransaction(3, 0L, 5.00, ZIP_CODE_3),
                        new DetailedTransaction(1, 0L, 750.50, ZIP_CODE_1)
                        // Add more transactions as needed...
                )
        );
    }

    /**
     * Checks whether there are more transactions to return.
     * - For bounded iterators, returns false when all transactions have been returned.
     * - For unbounded iterators, resets the index to loop through transactions again.
     *
     * @return true if there are more transactions, false otherwise
     */
    @Override
    public boolean hasNext() {
        if (runningIndex < detailedTransactions.size()) {
            return true;
        } else if (!bounded) {
            runningIndex = 0; // Reset index for unbounded iterators
            return true;
        } else {
            return false; // Stop if the iterator is bounded and the list is exhausted
        }
    }

    /**
     * Returns the next transaction in the list.
     * Sets the timestamp for the transaction and increments the running index.
     *
     * @return the next DetailedTransaction
     */
    @Override
    public DetailedTransaction next() { // NOSONAR
        DetailedTransaction detailedTransaction = detailedTransactions.get(runningIndex++);
        detailedTransaction.setTimestamp(timestamp);

        // Increment timestamp by 5 minutes to simulate transaction intervals
        timestamp += 5 * 60 * 1000; // 5 minutes
        return detailedTransaction;
    }
}
