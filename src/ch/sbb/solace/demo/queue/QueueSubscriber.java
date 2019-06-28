package ch.sbb.solace.demo.queue;

import java.util.concurrent.CountDownLatch;
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
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessageListener;

import ch.sbb.solace.demo.helper.MessageConstants;
import ch.sbb.solace.demo.helper.MessagePayloadHelper;
import ch.sbb.solace.demo.helper.SolaceHelper;

public class QueueSubscriber {

	private static AtomicInteger messageCount = new AtomicInteger(1);

	public static void main(final String... args) throws JCSMPException {
		SolaceHelper.setupLogging(Level.WARNING);

		final JCSMPSession session = SolaceHelper.connect();

		String queueName = SolaceHelper.TOPIC_DEMO;
		if (System.getProperty("queueName") != null) {
			queueName = System.getProperty("queueName");
		}
		
		System.out.println("subscribing: " + queueName);
		
		final Queue queue = createQueue(session, queueName);

		final CountDownLatch latch = new CountDownLatch(MessageConstants.SENDING_COUNT); // used for synchronizing b/w
																							// threads
		System.out.printf("Attempting to bind to the queue '%s' on the appliance.%n", queueName);

		// Create a Flow be able to bind to and consume messages from the Queue.
		final ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
		flow_prop.setEndpoint(queue);
		flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_AUTO);

		final EndpointProperties endpoint_props = new EndpointProperties();
		endpoint_props.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);

		final FlowReceiver cons = session.createFlow(new XMLMessageListener() {
			@Override
			public void onReceive(final BytesXMLMessage msg) {
				QueueSubscriber.onReceive(latch, msg);
			}

			@Override
			public void onException(final JCSMPException e) {
				System.out.printf("Consumer received exception: %s%n", e);
				latch.countDown(); // unblock main thread
			}
		}, flow_prop, endpoint_props);

		System.out.println("Connected. Awaiting message...");
		cons.start();

		try {
			latch.await(); // block until message received, and latch will flip
		} catch (final InterruptedException e) {
			System.out.println("I was awoken while waiting");
		}
		cons.close();
		System.out.println("Exiting.");
		session.closeSession();
	}

	private static void onReceive(final CountDownLatch latch, final BytesXMLMessage msg) {
		final int receivedMessagesCount = messageCount.getAndIncrement();
		if (msg instanceof TextMessage) {
			processTextMessage((TextMessage) msg, receivedMessagesCount);
		} else {
			processDefaultMessage(msg, receivedMessagesCount);
		}

		latch.countDown(); // unblock main thread
	}

	private static Queue createQueue(final JCSMPSession session, final String queueName) throws JCSMPException {
		System.out.printf("Attempting to provision the queue '%s' on the appliance.%n", queueName);
		final EndpointProperties endpointProps = new EndpointProperties();
		// set queue permissions to "consume" and access-type to "exclusive"
		endpointProps.setPermission(EndpointProperties.PERMISSION_CONSUME);
		endpointProps.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
		// create the queue object locally
		final Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
		// Actually provision it, and do not fail if it already exists
		session.provision(queue, endpointProps, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);

		return JCSMPFactory.onlyInstance().createQueue(queueName);
	}

	private static void processTextMessage(final TextMessage msg, final int count) {
		final String payload = msg.getText();
		MessagePayloadHelper.processPayload(payload, count);
	}

	private static void processDefaultMessage(final BytesXMLMessage msg, final int count) {
		final String countInfo = calcCountInfo(count);
		System.out.printf("%s Message received %n", countInfo);
	}

	private static String calcCountInfo(final int count) {
		return String.format(" [%d of %d] ", count, MessageConstants.SENDING_COUNT);
	}

}