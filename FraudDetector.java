/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spendreport;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;

/**
 * This class implements a simple fraud detection logic using Flink's
 * KeyedProcessFunction. It checks for a small transaction followed by
 * a large transaction to detect potential fraud.
 */
public class FraudDetector extends KeyedProcessFunction<Long, Transaction, Alert> {

	private static final long serialVersionUID = 1L;

	// Define threshold amounts for small and large transactions
	private static final double SMALL_AMOUNT = 1.00;
	private static final double LARGE_AMOUNT = 500.00;

	// Define time duration for checking fraud (1 minute)
	private static final long ONE_MINUTE = 60 * 1000L;

	// State variables to maintain the status of previous transactions and timers
	private transient ValueState<Boolean> flagState; // Flag to indicate if the last transaction was small
	private transient ValueState<Long> timerState; // Timer to track time between transactions

	/**
	 * This method processes each transaction element.
	 * It checks if the current transaction meets the criteria for potential fraud.
	 */
	@Override
	public void processElement(
			Transaction transaction,
			Context context,
			Collector<Alert> collector) throws Exception {

		// Check if the last transaction was small
		Boolean isLastTransactionSmall = flagState.value();

		// If the last transaction was small and the current transaction is large, flag as fraud
		if (isLastTransactionSmall != null) {
			if (transaction.getAmount() > LARGE_AMOUNT) {
				// Create an alert for potential fraud
				Alert alert = new Alert();
				alert.setId(transaction.getAccountId());
				collector.collect(alert); // Collect the alert
			}

			// Clean up the state and timer after detecting fraud
			cleanUp(context);
		}

		// If the current transaction is small, update the flag and set a timer
		if (transaction.getAmount() < SMALL_AMOUNT) {
			flagState.update(true); // Set the flag indicating a small transaction

			// Register a timer to clear the flag after 1 minute
			long timer = context.timerService().currentProcessingTime() + ONE_MINUTE;
			context.timerService().registerProcessingTimeTimer(timer);
			timerState.update(timer); // Update the timer state
		}
	}

	/**
	 * This method initializes the state variables when the function is opened.
	 */
	@Override
	public void open(Configuration parameters) {
		// State descriptor for the flag indicating a small transaction
		ValueStateDescriptor<Boolean> flagDescriptor = new ValueStateDescriptor<>(
				"flag",
				Types.BOOLEAN);
		flagState = getRuntimeContext().getState(flagDescriptor);

		// State descriptor for the timer state
		ValueStateDescriptor<Long> timerDescriptor = new ValueStateDescriptor<>(
				"timer-state",
				Types.LONG);
		timerState = getRuntimeContext().getState(timerDescriptor);
	}

	/**
	 * This method is triggered when the timer set earlier fires.
	 * It clears the flag state and timer state.
	 */
	@Override
	public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) {
		// Clear the states when the timer fires
		timerState.clear();
		flagState.clear();
	}

	/**
	 * This method clears the state and cancels the timer when needed.
	 */
	private void cleanUp(Context ctx) throws Exception {
		// Retrieve the current timer value and delete the event time timer
		Long timer = timerState.value();
		ctx.timerService().deleteEventTimeTimer(timer);

		// Clear the states
		timerState.clear();
		flagState.clear();
	}
}
