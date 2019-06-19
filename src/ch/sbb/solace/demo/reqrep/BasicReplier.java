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

import java.io.IOException;
import java.util.logging.Level;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;

import ch.sbb.solace.demo.helper.SolaceHelper;

public class BasicReplier {

	public static void main(final String... args) throws JCSMPException {
		SolaceHelper.setupLogging(Level.WARNING);
		final JCSMPProperties properties = SolaceHelper.setupProperties();
		System.out.println("BasicReplier initializing...");

		final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
		session.connect();

		final Topic topic = JCSMPFactory.onlyInstance().createTopic(SolaceHelper.TOPIC_PEQ_REP);

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

		final XMLMessageConsumer cons = session.getMessageConsumer(new XMLMessageListener() {
			@Override
			public void onReceive(final BytesXMLMessage request) {

				if (request.getReplyTo() != null) {
					System.out.println("Received request, generating response");
					final TextMessage reply = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);

					final String text = "Response |" + extractText(request);
					reply.setText(text);
					System.out.println("  " + text);

					try {
						producer.sendReply(request, reply);
					} catch (final JCSMPException e) {
						System.out.println("Error sending reply.");
						e.printStackTrace();
					}
				} else {
					System.out.println("Received message without reply-to field");
				}

			}

			public void onException(final JCSMPException e) {
				System.out.printf("Consumer received exception: %s%n", e);
			}
		});

		session.addSubscription(topic);
		cons.start();

		// Consume-only session is now hooked up and running!
		System.out.println("Listening for request messages on topic " + topic + " ... Press enter to exit");
		try {
			System.in.read();
		} catch (final IOException e) {
			e.printStackTrace();
		}

		// Close consumer
		cons.close();
		System.out.println("Exiting.");
		session.closeSession();
	}

	private static String extractText(final BytesXMLMessage msg) {
		if (msg instanceof TextMessage) {
			final String requestText = ((TextMessage) msg).getText();
			return requestText;
		}
		throw new UnsupportedOperationException(String.format("message of type %s not supported", msg.getClass().getSimpleName()));
	}
}
