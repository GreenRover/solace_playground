package ch.sbb.solace.demo.msgsize;

import java.io.IOException;
import java.util.logging.Level;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;

public class TopicPublisher {

	private static int messageId = 1;

	public static void main(String... args) throws JCSMPException, InterruptedException, IOException {
		SolaceHelper.setupLogging(Level.WARNING);
		JCSMPProperties properties = SolaceHelper.setupProperties();
		System.out.println("TopicPublisher initializing...");

		for (int i = 0; i < MessageConstants.SENDING_COUNT; i++) {
			runWithNewSession(i, properties, SolaceHelper.topicName, MessageConstants.DataType.K1_TextMessage);
		}
		System.out.println("DONE");
	}

	private static void runWithNewSession(int i, JCSMPProperties properties, String topicName,
			MessageConstants.DataType dataType) {
		try {
			final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
			session.connect();

			final Topic topic = JCSMPFactory.onlyInstance().createTopic(topicName);
			XMLMessageProducer prod = session.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
				@Override
				public void responseReceived(String messageID) {
					System.out.println("Producer received response for msg: " + messageID);
				}

				@Override
				public void handleError(String messageID, JCSMPException e, long timestamp) {
					System.out.printf("Producer received error for msg: %s@%s - %s%n", messageID, timestamp, e);
				}
			});

			TextMessage msg = createMessage(dataType, messageId);
			prod.send(msg, topic);
			System.out.println(calcCountInfo(i) + "MessageId-" + messageId + " sent");
			messageId++;
			session.closeSession();
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	private static TextMessage createMessage(MessageConstants.DataType dataType, int id) throws IOException {
		switch (dataType) {
		case K1_TextMessage:
			return createTextMessage(id, 1000);
		case K10_TextMessage:
			return createTextMessage(id, 10_000);
		case K100_TextMessage:
			return createTextMessage(id, 100_000);
		case M1_TextMessage:
			return createTextMessage(id, 1_000_000);
		default:
			TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
			msg.setText("not supporetd message");
			return msg;
		}
	}

	private static TextMessage createTextMessage(int i, int numOfChars) {
		final String text = String.format("%d %s!", i, createStringOfSize(numOfChars));
		TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		msg.setText(text);
		return msg;
	}

	private static String createStringOfSize(int n) {
		StringBuilder outputBuffer = new StringBuilder(n);
		for (int i = 0; i < n; i++) {
			outputBuffer.append("x");
		}
		return outputBuffer.toString();
	}

	private static String calcCountInfo(int count) {
		return String.format(" [%d of %d] ", count, MessageConstants.SENDING_COUNT);
	}
}
