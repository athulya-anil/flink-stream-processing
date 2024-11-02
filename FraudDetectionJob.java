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

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;

/**
 * This is the main class for the fraud detection job using Apache Flink.
 * It sets up a Flink data stream to detect potentially fraudulent transactions
 * based on predefined criteria. It uses a custom process function to analyze
 * transaction streams and generate alerts.
 */
public class FraudDetectionJob {
	public static void main(String[] args) throws Exception {
		// Create the Flink streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		/**
		 * Add a source to the environment to generate a stream of transactions.
		 * TransactionSource is a custom source that generates transactions in real-time.
		 * Each transaction contains details such as account ID, amount, and timestamp.
		 */
		DataStream<Transaction> transactions = env
				.addSource(new TransactionSource()) // Generate transactions
				.name("transactions"); // Set name for the source in the job graph

		/**
		 * Define a data processing pipeline to detect fraud.
		 * 1. The transactions are grouped by account ID using keyBy.
		 * 2. The process function (FraudDetector) is applied to each key group.
		 * 3. The process function checks for a sequence of small-to-large transactions,
		 *    indicating potential fraud, and generates an alert if found.
		 */
		DataStream<Alert> alerts = transactions
				.keyBy(Transaction::getAccountId) // Group transactions by account ID
				.process(new FraudDetector()) // Apply the fraud detection logic
				.name("fraud-detector"); // Set name for the process function in the job graph

		/**
		 * Add a sink to send the generated alerts.
		 * AlertSink is a custom sink that outputs alerts, which could be to a console,
		 * a database, or another external system.
		 */
		alerts
				.addSink(new AlertSink()) // Output the alerts
				.name("send-alerts"); // Set name for the sink in the job graph

		// Execute the Flink job with the name "Fraud Detection"
		env.execute("Fraud Detection");
	}
}
