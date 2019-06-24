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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
import com.solacesystems.jcsmp.XMLMessageListener;

import ch.sbb.solace.demo.helper.MessageConstants;
import ch.sbb.solace.demo.helper.SolaceHelper;

public class ParallelQueueConsumer {

	private static AtomicInteger messageCountPerSecond = new AtomicInteger(0);
	private static AtomicInteger messageCount = new AtomicInteger(0);
	private static int MAX_MESSAGES = MessageConstants.MAX_PARALLEL_THREADS * MessageConstants.SENDING_COUNT;

	public static void main(final String... args) throws JCSMPException, InterruptedException {
		SolaceHelper.setupLogging(Level.WARNING);
		System.out.println("ParallelQueueConsumer initializing...");
		final JCSMPProperties properties = SolaceHelper.setupProperties();
		final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
		session.connect();

		final RandomSelector rand = new RandomSelector();
		for (final Queue queue : rand.getAllQueues()) {
			// Actually provision it, and do not fail if it already exists
			session.provision(queue, createEndpointProperties(), JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
		}

		final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
		executorService.scheduleAtFixedRate(() -> {
			final int countPerSecond = messageCountPerSecond.getAndSet(0);
			final int countTotal = messageCount.get();
			System.out.printf("%d msg/s [%d | %d]%n", countPerSecond, countTotal, MAX_MESSAGES);
		}, 0, 1, TimeUnit.SECONDS);

		while (true) {
			for (final Queue queue : rand.getAllQueues()) {
				final FlowReceiver cons = createReceiver(session, queue);
				cons.start();
			}
		}
	}

	private static FlowReceiver createReceiver(final JCSMPSession session, final Queue queue) throws JCSMPException {
		// With a connected session, you then need to bind to the Solace message
		// router queue with a flow receiver. Flow receivers allow applications
		// to receive messages from a Solace guaranteed message flow.
		final FlowReceiver cons = session.createFlow(new XMLMessageListener() {
			@Override
			public void onReceive(final BytesXMLMessage msg) {
				messageCountPerSecond.incrementAndGet();
				messageCount.incrementAndGet();
				// When the ack mode is set to SUPPORTED_MESSAGE_ACK_CLIENT,
				// guaranteed delivery messages are acknowledged after
				// processing
				msg.ackMessage();
			}

			@Override
			public void onException(final JCSMPException e) {
				System.out.printf("Consumer received exception: %s%n", e);
			}
		}, createFlowProperties(queue), createEndpointProperties2());
		return cons;
	}

	private static ConsumerFlowProperties createFlowProperties(final Queue queue) {
		// Create a Flow be able to bind to and consume messages from the Queue.
		final ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
		flow_prop.setEndpoint(queue);
		flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
		return flow_prop;
	}

	private static EndpointProperties createEndpointProperties() {
		// set queue permissions to "consume" and access-type to "exclusive"
		final EndpointProperties endpointProps = new EndpointProperties();
		endpointProps.setPermission(EndpointProperties.PERMISSION_CONSUME);
		endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
		return endpointProps;
	}

	private static EndpointProperties createEndpointProperties2() {
		final EndpointProperties endpointProps = new EndpointProperties();
		endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
		return endpointProps;
	}
}
