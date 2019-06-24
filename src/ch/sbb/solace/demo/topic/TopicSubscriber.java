package ch.sbb.solace.demo.topic;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;

import ch.sbb.solace.demo.helper.MessageConstants;
import ch.sbb.solace.demo.helper.MessagePayloadHelper;
import ch.sbb.solace.demo.helper.SolaceHelper;

public class TopicSubscriber {

	private static AtomicInteger messageCount = new AtomicInteger(1);

	public static void main(final String... args) throws JCSMPException {
		SolaceHelper.setupLogging(Level.WARNING);
		final JCSMPSession session = SolaceHelper.connect();
		
		String queueName = SolaceHelper.TOPIC_DEMO;
		if (System.getProperty("queueName") != null) {
			queueName = System.getProperty("queueName");
		}
		
		System.out.println("subscribing: " + queueName);

		System.out.println("TopicSubscriber initializing...");
		final Topic topic = JCSMPFactory.onlyInstance().createTopic(queueName);

		final CountDownLatch latch = new CountDownLatch(MessageConstants.SENDING_COUNT);
		final XMLMessageConsumer cons = session.getMessageConsumer(new XMLMessageListener() {
			@Override
			public void onReceive(final BytesXMLMessage msg) {
				final int receivedMessagesCount = messageCount.getAndIncrement();
				if (msg instanceof TextMessage) {
					processTextMessage((TextMessage) msg, receivedMessagesCount);
				} else {
					processDefaultMessage(msg, receivedMessagesCount);
				}

				latch.countDown(); // unblock main thread
			}

			@Override
			public void onException(final JCSMPException e) {
				System.out.printf("Consumer received exception: %s%n", e);
				latch.countDown(); // unblock main thread
			}
		});
		session.addSubscription(topic);
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