/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package ch.sbb.solace.demo.queue.parallel;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;

import ch.sbb.solace.demo.helper.MessageConstants;
import ch.sbb.solace.demo.helper.MessagePayloadHelper;
import ch.sbb.solace.demo.helper.SolaceHelper;

public class ParallelQueueProducer {

	private static final AtomicInteger messageCount = new AtomicInteger();
	private static final RandomSelector rand = new RandomSelector();

	public static void main(final String... args) throws JCSMPException, InterruptedException {
		SolaceHelper.setupLogging(Level.WARNING);
		System.out.println("ParallelQueueProducer initializing...");

		final JCSMPProperties properties = SolaceHelper.setupProperties();
		
		// monitor sending statistics
		final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
		executorService.scheduleAtFixedRate(() -> {
			final int count = messageCount.getAndSet(0);
			System.out.printf("%d msg/s%n", count);
		}, 0, 1, TimeUnit.SECONDS);

		runInParallel(properties);
		System.out.println("DONE");
	}

	private static void runInParallel(final JCSMPProperties properties) throws JCSMPException {
		final ExecutorService executor = Executors.newFixedThreadPool(MessageConstants.MAX_PARALLEL_THREADS);
		for (int i = 0; i < MessageConstants.MAX_PARALLEL_THREADS; i++) {
			executor.submit(run(properties));
		}
		executor.shutdown();
	}

	private static Runnable run(final JCSMPProperties properties) {
		return new Runnable() {
			@Override
			public void run() {
				try {
					final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
					session.connect();
					configureQueues(session);
					final XMLMessageProducer prod = createProducer(session);

					for (int i = 1; i <= MessageConstants.SENDING_COUNT; i++) {
						final Queue queue = rand.getRandomQueue();
						final String text = rand.getRandomMessage();
						final TextMessage msg = createMessage(text, i, queue.getName());
						msg.setPriority(i % 10);
						prod.send(msg, queue);
						messageCount.incrementAndGet();
					}
					session.closeSession();
				} catch (final Exception e) {
					System.out.println(e);
				}
			}
		};
	}

	private static final void configureQueues(final JCSMPSession session) throws JCSMPException {
		// set queue permissions to "consume" and access-type to"exclusive"
		final EndpointProperties endpointProps = new EndpointProperties();
		endpointProps.setPermission(EndpointProperties.PERMISSION_CONSUME);
		endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);

		for (final Queue queue : rand.getAllQueues()) {
			// Actually provision it, and do not fail if it already exists
			session.provision(queue, endpointProps, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
		}
	}

	private static XMLMessageProducer createProducer(final JCSMPSession session) throws JCSMPException {
		// Solace message router will acknowledge the message once
		// it is successfully stored on the message router.
		final XMLMessageProducer prod = session.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
			@Override
			public void responseReceived(final String messageID) {
				// System.out.printf(" Producer received response for msg ID
				// #%s%n", messageID);
			}

			@Override
			public void handleError(final String messageID, final JCSMPException e, final long timestamp) {
				System.out.printf("  Producer received error for msg ID %s @ %s - %s%n", messageID, timestamp, e);
			}
		});
		return prod;
	}

	private static TextMessage createMessage(final String text, final int i, final String queueName) {
		final String payload = MessagePayloadHelper.createPayload(text, i, queueName);
		final TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		msg.setDeliveryMode(DeliveryMode.PERSISTENT);
		msg.setText(payload);
		return msg;
	}
}
