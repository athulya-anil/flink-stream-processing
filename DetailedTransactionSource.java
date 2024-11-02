package spendreport.detailed.source;

import org.apache.flink.annotation.Public;
import org.apache.flink.streaming.api.functions.source.FromIteratorFunction;
import spendreport.detailed.model.DetailedTransaction;

import java.io.Serializable;
import java.util.Iterator;

/**
 * A custom Flink source function that generates detailed transactions using an iterator.
 * This source generates a continuous stream of transactions for testing fraud detection logic.
 *
 * The source can be either bounded or unbounded:
 * - Bounded: Returns a finite number of transactions and stops.
 * - Unbounded: Continues generating transactions indefinitely in a round-robin fashion.
 *
 * It uses the FromIteratorFunction from Flink's API, which allows using an iterator as a source.
 */
@Public
public class DetailedTransactionSource extends FromIteratorFunction<DetailedTransaction> {
    private static final long serialVersionUID = 1L;

    /**
     * Constructor to create a new DetailedTransactionSource.
     * It wraps a rate-limited iterator around a bounded or unbounded transaction iterator.
     *
     * @param bounded - true if the source should generate a bounded stream, false for unbounded
     */
    public DetailedTransactionSource(boolean bounded) {
        super(new RateLimitedIterator<>(
                bounded ? DetailedTransactionIterator.bounded() : DetailedTransactionIterator.unbounded()
        ));
    }

    /**
     * A rate-limited iterator that limits the rate at which transactions are generated.
     * It introduces a delay of 100 milliseconds between each transaction to simulate
     * real-time transaction generation.
     *
     * @param <T> - the type of elements returned by this iterator (DetailedTransaction)
     */
    private static class RateLimitedIterator<T> implements Iterator<T>, Serializable {
        private static final long serialVersionUID = 1L;
        private final Iterator<T> inner; // The wrapped iterator for transaction generation

        /**
         * Constructor to initialize the rate-limited iterator.
         *
         * @param inner - the original transaction iterator (bounded or unbounded)
         */
        private RateLimitedIterator(Iterator<T> inner) {
            this.inner = inner;
        }

        /**
         * Checks if there are more transactions available.
         * Delegates the call to the wrapped iterator.
         *
         * @return true if there are more transactions, false otherwise
         */
        public boolean hasNext() {
            return this.inner.hasNext();
        }

        /**
         * Returns the next transaction from the iterator.
         * Introduces a delay of 100 milliseconds to limit the rate.
         *
         * @return the next DetailedTransaction
         */
        public T next() {
            try {
                // Introduce a delay to simulate real-time transaction generation
                Thread.sleep(100L);
            } catch (InterruptedException e) { // Handle interrupted exception
                throw new RuntimeException(e); // Convert to a runtime exception
            }

            // Return the next transaction from the wrapped iterator
            return this.inner.next();
        }
    }
}
