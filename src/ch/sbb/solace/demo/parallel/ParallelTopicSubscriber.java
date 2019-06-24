package ch.sbb.solace.demo.parallel;

import java.text.NumberFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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

import ch.sbb.solace.demo.helper.MessageConstants;
import ch.sbb.solace.demo.helper.MessagePayloadHelper;
import ch.sbb.solace.demo.helper.SolaceHelper;

public class ParallelTopicSubscriber {

	private static AtomicInteger messageCount = new AtomicInteger(1);
	private static boolean isDebug = false;
	
	// stats about priorities of received messages
	private static Map<Integer, Integer> map = new ConcurrentHashMap<>();
	
	public static void main(final String... args) throws JCSMPException {
		SolaceHelper.setupLogging(Level.WARNING);
		final JCSMPProperties properties = SolaceHelper.setupProperties();

		System.out.println("TopicSubscriber initializing...");
		final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
		session.connect();

		final CountDownLatch latch = new CountDownLatch(
				MessageConstants.SENDING_COUNT * MessageConstants.MAX_PARALLEL_THREADS);
		final XMLMessageConsumer cons = session.getMessageConsumer(new XMLMessageListener() {
			@Override
			public void onReceive(final BytesXMLMessage msg) {
				final int receivedMessagesCount = messageCount.getAndIncrement();
				map.compute(msg.getPriority(), (k,v) -> (v == null) ? 1: v+1);
				
				if (isDebug) {
					if (msg instanceof TextMessage) {
						processTextMessage((TextMessage) msg, receivedMessagesCount);
					} else {
						processDefaultMessage(msg, receivedMessagesCount);
					}
				}
				messageCount.incrementAndGet();
				latch.countDown(); // unblock main thread
			}

			@Override
			public void onException(final JCSMPException e) {
				System.out.printf("Consumer received exception: %s%n", e);
				latch.countDown(); // unblock main thread
			}
		});
		RandomSelector rand = new RandomSelector();
		for (Topic topic : rand.getAllTopics()) {
			session.addSubscription(topic);
		}

		System.out.println("Connected. Awaiting message...");
		cons.start();

		NumberFormat nf = NumberFormat.getInstance();
		ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
		executorService.scheduleAtFixedRate(() -> {
			int count = messageCount.getAndSet(0);
			System.out.println(nf.format(count) + " msg/s");
		}, 0, 1, TimeUnit.SECONDS);

		executorService.scheduleAtFixedRate(() -> {
			System.out.printf("  message prio stats: %s%n", map);
		}, 0, 30, TimeUnit.SECONDS);
		
		try {
			latch.await(); // block until message received, and latch will flip
		} catch (final InterruptedException e) {
			System.out.println("I was awoken while waiting");
		}
		cons.close();
		System.out.println("Exiting.");
		System.out.printf("received %d messages", messageCount.get());
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