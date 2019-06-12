package ch.sbb.solace.demo.msgsize;

import java.io.IOException;
import java.util.Calendar;
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

	private static String MESSAGE_K1 = createStringOfSize(1000);
	private static String MESSAGE_K10 = createStringOfSize(10_000);
	private static String MESSAGE_K100 = createStringOfSize(100_000);
	private static String MESSAGE_K1000 = createStringOfSize(1_000_000);

	public static void main(String... args) throws JCSMPException, InterruptedException, IOException {
		SolaceHelper.setupLogging(Level.WARNING);
		JCSMPProperties properties = SolaceHelper.setupProperties();
		System.out.println("TopicPublisher initializing...");

		runWithNewSession(properties, SolaceHelper.topicName, MessageConstants.DataType.K100_TextMessage);
		System.out.println("DONE");
	}

	private static void runWithNewSession(JCSMPProperties properties, String topicName,
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

			for (int i = 1; i <= MessageConstants.SENDING_COUNT; i++) {
				TextMessage msg = createMessage(dataType, i);
				prod.send(msg, topic);
				System.out.println(calcCountInfo(i) + "MessageId-" + i + " sent");
			}
			session.closeSession();
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	private static TextMessage createMessage(MessageConstants.DataType dataType, int id) throws IOException {
		return createTextMessage(id, dataType);

	}

	private static TextMessage createTextMessage(int i, MessageConstants.DataType dataType) {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(Calendar.getInstance().getTimeInMillis() + " ");
		switch (dataType) {
		case K1_TextMessage:
			stringBuilder.append(String.format("%d %s!", i, MESSAGE_K1));
			break;
		case K10_TextMessage:
			stringBuilder.append(String.format("%d %s!", i, MESSAGE_K10));
			break;
		case K100_TextMessage:
			stringBuilder.append(String.format("%d %s!", i, MESSAGE_K100));
			break;
		case K1000_TextMessage:
			stringBuilder.append(String.format("%d %s!", i, MESSAGE_K1000));
			break;
		default:
			TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
			msg.setText("not supporetd message");
			return msg;
		}
		TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		msg.setText(stringBuilder.toString());
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
