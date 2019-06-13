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

	public static void main(String... args) throws JCSMPException, InterruptedException, IOException {
		SolaceHelper.setupLogging(Level.WARNING);
		JCSMPProperties properties = SolaceHelper.setupProperties();
		System.out.println("TopicPublisher initializing...");

		runWithNewSession(properties, SolaceHelper.TOPIC_MYCLASS_1_0, MessageConstants.DataType.K100_TextMessage);
		// runWithNewSession(properties, SolaceHelper.TOPIC_MYCLASS_2_0,
		// MessageConstants.DataType.K10_TextMessage);
		// runWithNewSession(properties, SolaceHelper.TOPIC_YOURCLASS_1_0,
		// MessageConstants.DataType.K100_TextMessage);

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
				TextMessage msg = createMessage(dataType, i, topic.getName());
				prod.send(msg, topic);
				System.out.println(calcCountInfo(i) + "MessageId-" + i + " sent");
			}
			session.closeSession();
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	private static TextMessage createMessage(MessageConstants.DataType dataType, int i, String topicName) {
		final String payload = MessagePayloadHelper.createPayload(dataType, i, topicName);
		return createTextMessage(payload);
	}

	private static TextMessage createTextMessage(String text) {
		TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		msg.setText(text);
		return msg;
	}

	private static String calcCountInfo(final int count) {
		return String.format(" [%d of %d] ", count, MessageConstants.SENDING_COUNT);
	}
}
