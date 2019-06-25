package ch.sbb.solace.demo.topic.parallel;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.Consumer;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;

import ch.sbb.solace.demo.parallel.base.ParallelReceiver;
import ch.sbb.solace.demo.parallel.base.RandomSelector;

public class ParallelTopicSubscriber extends ParallelReceiver {

	private static final RandomSelector rand = new RandomTopicSelector();

	public ParallelTopicSubscriber() {
		super("ParallelTopicSubscriber", rand);
	}

	public void configureSession(final JCSMPSession session, final Destination destination) throws JCSMPException {
		final Topic topic = (Topic) destination;
		System.out.print(".");
		session.addSubscription(topic);
	}

	public Consumer createReceiver(final JCSMPSession session, final Destination destination) throws JCSMPException {
		final XMLMessageConsumer cons = session.getMessageConsumer(new XMLMessageListener() {
			@Override
			public void onReceive(final BytesXMLMessage msg) {
				messageCountPerSecond.incrementAndGet();
				messageCount.incrementAndGet();
				map.compute(msg.getPriority(), (k, v) -> (v == null) ? 1 : v + 1);
			}

			@Override
			public void onException(final JCSMPException e) {
				System.out.printf("Consumer received exception: %s%n", e);
			}
		});
		return cons;

	}

	public static void main(final String[] args) throws Exception {
		new ParallelTopicSubscriber().go();
	}

}