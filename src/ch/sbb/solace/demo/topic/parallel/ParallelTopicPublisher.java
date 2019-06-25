package ch.sbb.solace.demo.topic.parallel;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;

import ch.sbb.solace.demo.helper.MessagePayloadHelper;
import ch.sbb.solace.demo.parallel.base.ParallelSender;
import ch.sbb.solace.demo.parallel.base.RandomSelector;

public class ParallelTopicPublisher extends ParallelSender {

	private static final RandomSelector rand = new RandomTopicSelector();

	public ParallelTopicPublisher() {
		super("ParallelTopicPublisher", rand);
	}

	@Override
	public XMLMessageProducer createProducer(final JCSMPSession session) throws JCSMPException {
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
		return prod;
	}

	@Override
	public TextMessage createMessage(final String text, final int i, final String topicName) {
		final String payload = MessagePayloadHelper.createPayload(text, i, topicName);
		final TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		msg.setText(payload);
		return msg;
	}

	@Override
	public void configureSession(final JCSMPSession session) throws JCSMPException {
	}

	public static void main(final String[] args) throws Exception {
		new ParallelTopicPublisher().go();
	}
}
