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

package ch.sbb.solace.demo.queue;

import java.text.DateFormat;
import java.util.Date;
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
import ch.sbb.solace.demo.helper.SolaceHelper;

public class QueueProducer {

	public static void main(final String... args) throws JCSMPException, InterruptedException {
		SolaceHelper.setupLogging(Level.WARNING);
		System.out.println("QueueProducer initializing...");
		final JCSMPProperties properties = SolaceHelper.setupProperties();
		final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);

		session.connect();

		final String queueName = SolaceHelper.QUEUE_NAME;
		System.out.printf("Attempting to provision the queue '%s' on the appliance.%n", queueName);

		final EndpointProperties endpointProps = new EndpointProperties();
		// set queue permissions to "consume" and access-type to "exclusive"
		endpointProps.setPermission(EndpointProperties.PERMISSION_CONSUME);
		endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);

		// create the queue object locally
		final Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
		
		// Actually provision it, and do not fail if it already exists
		session.provision(queue, endpointProps, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);

		/** Anonymous inner-class for handling publishing events */
		final XMLMessageProducer prod = session.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
			@Override
			public void responseReceived(final String messageID) {
				System.out.printf("  Producer received response for msg ID #%s%n", messageID);
			}

			@Override
			public void handleError(final String messageID, final JCSMPException e, final long timestamp) {
				System.out.printf("  Producer received error for msg ID %s @ %s - %s%n", messageID, timestamp, e);
			}
		});

		// Publish-only session is now hooked up and running!
		System.out.printf("Connected. About to send message to queue '%s'...%n", queue.getName());

		for (int i = 0; i < MessageConstants.MAX_MESSAGES_IN_QUEUE; i++) {
			final TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
			msg.setDeliveryMode(DeliveryMode.PERSISTENT);
			String now = DateFormat.getDateTimeInstance().format(new Date());
			final String text = String.format("%d | Persistent Queue Tutorial! %s", i, now);
			msg.setText(text);

			// Send message directly to the queue
			System.out.printf("sending '%s' %n", text);
			prod.send(msg, queue);
		}

		// Delivery not yet confirmed. See ConfirmedPublish.java
		System.out.println("All message sent. Exiting.");

		// Close session
		session.closeSession();
	}
}
