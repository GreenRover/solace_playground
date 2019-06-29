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

package ch.sbb.solace.demo.reqrep;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.Requestor;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;

import ch.sbb.solace.demo.helper.MessageConstants;
import ch.sbb.solace.demo.helper.SolaceHelper;

/**
 * Requestor sends several requests (in separate threads) to the replier and
 * waits for the result. The requestor may run into some timeout.
 * 
 * <p>
 * Use this class in combination with the TimeoutReplier.
 */

public class TimeoutRequestor {

	private static AtomicInteger msgCount = new AtomicInteger(1);

	public static void main(final String... args) throws JCSMPException {
		SolaceHelper.setupLogging(Level.WARNING);
		final JCSMPProperties properties = SolaceHelper.setupProperties();
		System.out.println("BasicRequestor initializing...");

		final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
		session.connect();

		@SuppressWarnings("unused")
		final XMLMessageProducer producer = session.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
			@Override
			public void responseReceived(final String messageID) {
				System.out.println("Producer received response for msg: " + messageID);
			}

			@Override
			public void handleError(final String messageID, final JCSMPException e, final long timestamp) {
				System.out.printf("Producer received error for msg: %s@%s - %s%n", messageID, timestamp, e);
			}
		});

		final XMLMessageConsumer consumer = session.getMessageConsumer((XMLMessageListener) null);
		consumer.start();

		final Topic topic = JCSMPFactory.onlyInstance().createTopic(SolaceHelper.TOPIC_PEQ_REP);
		final int timeoutMs = MessageConstants.REQUEST_TIMEOUT_IN_MILLIS;

		final ExecutorService executor = Executors.newFixedThreadPool(MessageConstants.PARALLEL_THREADS);
		for (int i = 0; i < 10; i++) {
			executor.submit(run(session, topic, timeoutMs));
		}

		executor.shutdown();
		session.closeSession();
	}

	private static Runnable run(final JCSMPSession session, final Topic topic, final int timeoutMs) {
		return new Runnable() {

			@Override
			public void run() {
				final int id = msgCount.getAndIncrement();
				final String text = String.format("Sample Request | %2d", id);
				try {
					delayStaring(id);
					final Requestor requestor = session.createRequestor();
					System.out.printf("%2d | Sending request %n", id);
					final TextMessage request = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
					request.setText(text);

					System.out.printf("%2d |   Connected. About to send request message '%s' to topic '%s'...%n", id,
							text, topic.getName());

					final BytesXMLMessage reply = requestor.request(request, timeoutMs, topic);
					if (reply instanceof TextMessage) {
						System.out.printf("%2d | TextMessage response received: '%s'%n", id,
								((TextMessage) reply).getText());
					}

				} catch (final JCSMPException e) {
					System.out.printf("%2d | Failed to receive a reply for request '%s' in %d msecs%n", id, text,
							timeoutMs);
				}
			}

		};
	}

	private static void delayStaring(int id) {
		try {
			Thread.sleep(id * 500);
		} catch (final InterruptedException e) {
			e.printStackTrace();
		}
	}

}
