package ch.sbb.solace.demo.msgsize;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;

public class TopicSubscriber {

	private static AtomicInteger messageCount = new AtomicInteger(1);

	public static void main(String... args) throws JCSMPException {
		SolaceHelper.setupLogging(Level.WARNING);
		JCSMPProperties properties = SolaceHelper.setupProperties();

		System.out.println("TopicSubscriber initializing...");
		final Topic topic = JCSMPFactory.onlyInstance().createTopic(SolaceHelper.topicName);
		final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
		session.connect();

		final CountDownLatch latch = new CountDownLatch(MessageConstants.SENDING_COUNT);
		final XMLMessageConsumer cons = session.getMessageConsumer(new XMLMessageListener() {
			@Override
			public void onReceive(BytesXMLMessage msg) {
				int receivedMessagesCount = messageCount.getAndIncrement();
				if (msg instanceof TextMessage) {
					processTextMessage((TextMessage) msg, receivedMessagesCount);
				} else {
					processDefaultMessage(msg, receivedMessagesCount);
				}

				latch.countDown(); // unblock main thread
			}

			@Override
			public void onException(JCSMPException e) {
				System.out.printf("Consumer received exception: %s%n", e);
				latch.countDown(); // unblock main thread
			}
		});
		session.addSubscription(topic);
		System.out.println("Connected. Awaiting message...");
		cons.start();

		try {
			latch.await(); // block until message received, and latch will flip
		} catch (InterruptedException e) {
			System.out.println("I was awoken while waiting");
		}
		cons.close();
		System.out.println("Exiting.");
		session.closeSession();
	}

	private static String calcCountInfo(int count) {
		return String.format(" [%d of %d] ", count, MessageConstants.SENDING_COUNT);
	}

	private static void processTextMessage(TextMessage msg, int count) {
		String countInfo = calcCountInfo(count);
		int msgLength = msg.getText().length();
		System.out.printf("%s TextMessage received: %s | %d bytes %n", countInfo, extractMessageInfo(msg), msgLength);
	}

	private static String extractMessageInfo(TextMessage msg) {
		String s = msg.getText();
		if (Objects.isNull(s)) {
			return "nix";
		} else if (s.length() > 20) {
			return s.substring(0, 20);
		} else {
			return String.format("%s%nMessage Dump:%n%s%n", s, msg.dump());
		}
	}

	private static void processDefaultMessage(BytesXMLMessage msg, int count) {
		String countInfo = calcCountInfo(count);
		System.out.printf("%s Message received %n", countInfo);
		// System.out.printf("Message Dump:%n%s%n", msg.dump(20));
	}
}