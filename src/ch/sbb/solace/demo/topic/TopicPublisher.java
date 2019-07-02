package ch.sbb.solace.demo.topic;

import java.io.IOException;
import java.util.logging.Level;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;

import ch.sbb.solace.demo.helper.MessageConstants;
import ch.sbb.solace.demo.helper.MessagePayloadHelper;
import ch.sbb.solace.demo.helper.SolaceHelper;

public class TopicPublisher {

	public static void main(final String... args) throws JCSMPException, InterruptedException, IOException {
		SolaceHelper.setupLogging(Level.FINER);
		final JCSMPSession session = SolaceHelper.connect();
		System.out.println("TopicPublisher initializing...");

		String queueName = SolaceHelper.TOPIC_DEMO;
		if (System.getProperty("queueName") != null) {
			queueName = System.getProperty("queueName");
		}

		MessageConstants.DataType msgSize = MessageConstants.DataType.K10_TextMessage;
		if (System.getProperty("msgSize") != null) {
			msgSize = MessageConstants.DataType.valueOf(System.getProperty("msgSize"));
		}

		System.out.println(
				"sending: " + MessageConstants.SENDING_COUNT + "msgs, with " + msgSize.name() + " to " + queueName);

		runWithNewSession(session, queueName, msgSize);
		// runWithNewSession(properties, SolaceHelper.TOPIC_MYCLASS_2_0,
		// MessageConstants.DataType.K10_TextMessage);
		// runWithNewSession(properties, SolaceHelper.TOPIC_YOURCLASS_1_0,
		// MessageConstants.DataType.K100_TextMessage);

		System.out.println("DONE");
	}

	private static void runWithNewSession(final JCSMPSession session, final String topicName,
			final MessageConstants.DataType dataType) {
		try {
			final Topic topic = JCSMPFactory.onlyInstance().createTopic(topicName);
			final XMLMessageProducer prod = session.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
				@Override
				public void responseReceived(final String messageID) {
					System.out.println("Producer received response for msg: " + messageID);
				}

				@Override
				public void handleError(final String messageID, final JCSMPException e, final long timestamp) {
					System.out.printf("Producer received error for msg: %s@%s - %s%n", messageID, timestamp, e);
				}
			});
			
			final int delay = Integer.parseInt(System.getProperty("delay"));

			for (int i = 1; i <= MessageConstants.SENDING_COUNT; i++) {
				final TextMessage msg = createMessage(dataType, i, topic.getName());
//				msg.setDeliveryMode(DeliveryMode.PERSISTENT);
//				msg.setTimeToLive(10 * 1000);
				prod.send(msg, topic);
				System.out.println(calcCountInfo(i) + "MessageId-" + i + " sent");
				
				if (delay > 0) {
					Thread.sleep(delay);
				}
			}
			session.closeSession();
		} catch (final Exception e) {
			System.out.println(e);
		}
	}

	private static TextMessage createMessage(final MessageConstants.DataType dataType, final int i,
			final String topicName) {
		final String payload = MessagePayloadHelper.createPayload(dataType, i, topicName);
		return createTextMessage(payload);
	}

	private static TextMessage createTextMessage(final String text) {
		final TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		msg.setText(text);
		return msg;
	}

	private static String calcCountInfo(final int count) {
		return String.format(" [%d of %d] ", count, MessageConstants.SENDING_COUNT);
	}
}
