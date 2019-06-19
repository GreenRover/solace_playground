package ch.sbb.solace.demo.parallel;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;

import ch.sbb.solace.demo.helper.MessageConstants;
import ch.sbb.solace.demo.helper.MessagePayloadHelper;
import ch.sbb.solace.demo.helper.SolaceHelper;

public class ParallelTopicPublisher {

	private static AtomicInteger msgCount = new AtomicInteger();
	private static final boolean DEBUG = false;

	public static void main(final String... args) throws JCSMPException, InterruptedException, IOException {
		SolaceHelper.setupLogging(Level.WARNING);
		final JCSMPProperties properties = SolaceHelper.setupProperties();
		System.out.println("TopicPublisher initializing...");

		NumberFormat nf = NumberFormat.getInstance();
		ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
		executorService.scheduleAtFixedRate(() -> {
			int count = msgCount.getAndSet(0);
			System.out.println(nf.format(count) + " msg/s");
		}, 0, 1, TimeUnit.SECONDS);

		runInParallel(properties);
		System.out.println("DONE");
	}

	private static void runInParallel(JCSMPProperties properties) throws JCSMPException {
		ExecutorService executor = Executors.newFixedThreadPool(MessageConstants.MAX_PARALLEL_THREADS);
		for (int i = 0; i < MessageConstants.MAX_PARALLEL_THREADS; i++) {
			executor.submit(run(properties));
		}
		executor.shutdown();
	}

	private static Runnable run(final JCSMPProperties properties) {
		return new Runnable() {
			@Override
			public void run() {
				try {
					final RandomSelector rand = new RandomSelector();
					final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
					session.connect();

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

					for (int i = 1; i <= MessageConstants.SENDING_COUNT; i++) {
						final Topic topic = rand.getRandomTopic();
						String text = rand.getRandomMessage();
						final TextMessage msg = createMessage(rand.getRandomMessage(), i, topic.getName());
						prod.send(msg, topic);
						msgCount.incrementAndGet();
						if (DEBUG) {
							System.out.println(calcCountInfo(i) //
									+ "MessageId-" + i //
									+ " sent. | " //
									+ topic.getName() + " | " //
									+ text.length() + " | " //
									+ Thread.currentThread().getName());
						}
					}
					session.closeSession();
				} catch (final Exception e) {
					System.out.println(e);
				}
			}
		};
	}

	private static TextMessage createMessage(final String text, final int i, final String topicName) {
		final String payload = MessagePayloadHelper.createPayload(text, i, topicName);
		final TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		msg.setText(payload);
		return msg;
	}

	private static String calcCountInfo(final int count) {
		return String.format(" [%d of %d] ", count, MessageConstants.SENDING_COUNT);
	}
}
