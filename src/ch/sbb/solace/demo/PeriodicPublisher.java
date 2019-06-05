package ch.sbb.solace.demo;

import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;

import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;

public class PeriodicPublisher {

	final int count = 5;
	final CountDownLatch latch = new CountDownLatch(count); // used for synchronizing b/w threads

	public static void main(String[] args) throws JCSMPException {
		new PeriodicPublisher();
	}

	public PeriodicPublisher() throws JCSMPException {
		final String queueName = "test/queue";

		System.out.println("HelloWorldPub initializing... using: " + Charset.defaultCharset().displayName());

		// Create a JCSMP Session
		final JCSMPSession session = ConnectionFactory.getSession();

		/** Correlating event handler */
//        final XMLMessageProducer prod = session.getMessageProducer(new PubCallback());

		final Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);

		/** Anonymous inner-class for handling publishing events */
		final XMLMessageProducer prod = session.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
			public void responseReceived(String messageID) {
				System.out.printf("Producer received response for msg: \"%s\"%n", messageID);
			}
			@Override
			public void responseReceivedEx(Object key) {
				System.out.printf("Producer receivedEx response for msg: \"%s\"%n", key);
			}

			public void handleError(String messageID, JCSMPException e, long timestamp) {
				System.out.printf("Producer received error for msg: %s@%s - %s%n", messageID, timestamp, e);
			}

			@Override
			public void handleErrorEx(Object key, JCSMPException e, long timestamp) {
				System.out.printf("Producer receivedEx error for msg: %s@%s - %s%n", key, timestamp, e);
			}

		});

		// Publish-only session is now hooked up and running!

		new Thread(() -> {
			for (int i = 0; i < 9999; i++) {
				TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
				final String text = "Hello wörld! " + i;
//				msg.setHTTPContentEncoding("ISO-8859-1");
				msg.setText(text);
				msg.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
				msg.setCorrelationKey(Integer.valueOf(i));
				msg.setTimeToLive(20);

				try {
					prod.send(msg, queue);
					System.out.println("Message \"" + text + "\" sent. With id: " + i); // msg.getMessageId()
					Thread.sleep(1000);
				} catch (JCSMPException | InterruptedException e1) {
					System.out.println("Unable to send msg: " + e1.getMessage());
				}
			}
		}).run();

		System.out.println("Exiting.");
		session.closeSession();
	}
}
