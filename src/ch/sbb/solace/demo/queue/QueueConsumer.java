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

import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessageListener;

import ch.sbb.solace.demo.helper.MessageConstants;
import ch.sbb.solace.demo.helper.SolaceHelper;

public class QueueConsumer {

	private static final boolean isDebug = false;

	public static void main(String... args) throws JCSMPException, InterruptedException {
		SolaceHelper.setupLogging(Level.WARNING);
		System.out.println("QueueConsumer initializing...");
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

		final CountDownLatch latch = new CountDownLatch(MessageConstants.MAX_MESSAGES_IN_QUEUE + 1); // used
																										// for
		// synchronizing
		// b/w threads

		System.out.printf("Attempting to bind to the queue '%s' on the appliance.%n", queueName);

		// Create a Flow be able to bind to and consume messages from the Queue.
		final ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
		flow_prop.setEndpoint(queue);
		flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);

		EndpointProperties endpoint_props = new EndpointProperties();
		endpoint_props.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);

		// With a connected session, you then need to bind to the Solace message
		// router queue with a flow receiver. Flow receivers allow applications
		// to receive messages from a Solace guaranteed message flow.
		final FlowReceiver cons = session.createFlow(new XMLMessageListener() {
			@Override
			public void onReceive(BytesXMLMessage msg) {
				if (msg instanceof TextMessage) {
					System.out.printf("TextMessage received: '%s'%n", ((TextMessage) msg).getText());
				} else {
					System.out.println("Message received.");
				}
				if (isDebug) {
					System.out.printf("Message Dump:%n%s%n", msg.dump());
				}

				// When the ack mode is set to SUPPORTED_MESSAGE_ACK_CLIENT,
				// guaranteed delivery messages are acknowledged after
				// processing
				msg.ackMessage();
				latch.countDown(); // unblock main thread
			}

			@Override
			public void onException(JCSMPException e) {
				System.out.printf("Consumer received exception: %s%n", e);
				latch.countDown(); // unblock main thread
			}
		}, flow_prop, endpoint_props);

		// Start the consumer
		System.out.println("Connected. Awaiting message ...");
		cons.start();

		try {
			latch.await(); // block here until message received, and latch will
							// flip
		} catch (InterruptedException e) {
			System.out.println("I was awoken while waiting");
		}
		// Close consumer
		cons.close();
		System.out.println("Exiting.");
		if (!session.isClosed()) {
			session.closeSession();
		}
	}
}
