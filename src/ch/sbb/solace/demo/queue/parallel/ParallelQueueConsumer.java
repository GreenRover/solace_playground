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

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.Consumer;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.XMLMessageListener;

import ch.sbb.solace.demo.parallel.base.ParallelReceiver;
import ch.sbb.solace.demo.parallel.base.RandomSelector;

public class ParallelQueueConsumer extends ParallelReceiver {

	private static final RandomSelector rand = new RandomQueueSelector();
	
	public ParallelQueueConsumer() {
		super("ParallelQueueConsumer", rand);
	}
	
	public void configureSession(final JCSMPSession session, final Destination destination) throws JCSMPException {		
		final Queue queue = (Queue)destination;

		// set queue permissions to "consume" and access-type to "exclusive"
		final EndpointProperties endpointProps = new EndpointProperties();
		endpointProps.setPermission(EndpointProperties.PERMISSION_CONSUME);
		endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
		
		session.provision(queue, endpointProps, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
	}
	
	public Consumer createReceiver(final JCSMPSession session, final Destination destination) throws JCSMPException {
		final Queue queue = (Queue)destination;
		
		// With a connected session, you then need to bind to the Solace message
		// router queue with a flow receiver. Flow receivers allow applications
		// to receive messages from a Solace guaranteed message flow.
		final FlowReceiver cons = session.createFlow(new XMLMessageListener() {
			@Override
			public void onReceive(final BytesXMLMessage msg) {
				messageCountPerSecond.incrementAndGet();
				messageCount.incrementAndGet();
				map.compute(msg.getPriority(), (k, v) -> (v == null) ? 1 : v + 1);
				
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

	private  ConsumerFlowProperties createFlowProperties(final Queue queue) {
		// Create a Flow be able to bind to and consume messages from the Queue.
		final ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
		flow_prop.setEndpoint(queue);
		flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
		return flow_prop;
	}


	private  EndpointProperties createEndpointProperties2() {
		final EndpointProperties endpointProps = new EndpointProperties();
		endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
		return endpointProps;
	}
	
	public static void main(final String[] args) throws Exception {
		new ParallelQueueConsumer().go();
	}
	
}
