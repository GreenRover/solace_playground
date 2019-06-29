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

import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;

import ch.sbb.solace.demo.helper.MessagePayloadHelper;
import ch.sbb.solace.demo.parallel.base.ParallelSender;
import ch.sbb.solace.demo.parallel.base.RandomSelector;

/**
 * Options:
 *              
 * -Dhost         Auth credentials
 * -Dvpn
 * -Duser
 * -Dpassword
 * 
 * -Dcount=10_000  Number of msg to send  0==MaxInt
 * -Dthreads=12	   Number of threads to use to send
 * -DminQueue=1    Queue to send data to
 * -DmaxQueue=50   Queue to send data to
 * -DmsgSize=0     The size of the msg to send in byte. 0 means random changing between 10b - 2000b
 */
public class ParallelQueueProducer extends ParallelSender {

	private static final RandomSelector rand = new RandomQueueSelector( //
			Integer.parseInt(System.getProperty("minQueue", "1")), //
			Integer.parseInt(System.getProperty("maxQueue", "50")) //
	);

	public ParallelQueueProducer() {
		super("ParallelQueueProducer", rand);
	}

	@Override
	public  void configureSession(final JCSMPSession session) throws JCSMPException {
		// set queue permissions to "consume" and access-type to"exclusive"
		final EndpointProperties endpointProps = new EndpointProperties();
		endpointProps.setPermission(EndpointProperties.PERMISSION_CONSUME);
		endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);

		for (final Destination queue : rand.getAllDestinations()) {
			// Actually provision it, and do not fail if it already exists
			session.provision((Queue)queue, endpointProps, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
		}		
	}

	@Override
	public  XMLMessageProducer createProducer(final JCSMPSession session) throws JCSMPException {
		// Solace message router will acknowledge the message once
		// it is successfully stored on the message router.
		final XMLMessageProducer prod = session.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
			@Override
			public void responseReceived(final String messageID) {
				// receives the ack
			}

			@Override
			public void handleError(final String messageID, final JCSMPException e, final long timestamp) {
				System.out.printf("  Producer received error for msg ID %s @ %s - %s%n", messageID, timestamp, e);
			}
		});
		return prod;
	}

	@Override
	public TextMessage createMessage(final String text, final int i, final String queueName) {
		final String payload = MessagePayloadHelper.createPayload(text, i, queueName);
		final TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		msg.setDeliveryMode(DeliveryMode.PERSISTENT);
		msg.setText(payload);
		return msg;
	}

	public static void main(final String[] args) throws Exception{
		new ParallelQueueProducer().go();
	}
}
