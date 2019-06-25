package ch.sbb.solace.demo.parallel.base;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;

import ch.sbb.solace.demo.helper.MessageConstants;
import ch.sbb.solace.demo.helper.SolaceHelper;

public abstract class ParallelSender {

	private final AtomicInteger messageCount = new AtomicInteger();
	private final RandomSelector rand;
	private final String name;

	public ParallelSender(final String name, final RandomSelector rand) {
		this.name = name;
		this.rand = rand;
	}
	
	public void go() throws JCSMPException, InterruptedException {
		SolaceHelper.setupLogging(Level.WARNING);
		System.out.printf("%s initializing...%n", name);

		final JCSMPProperties properties = SolaceHelper.setupProperties();
		
		// monitor sending statistics
		final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
		executorService.scheduleAtFixedRate(() -> {
			final int count = messageCount.getAndSet(0);
			System.out.printf("%d msg/s%n", count);
		}, 0, 1, TimeUnit.SECONDS);

		runInParallel(properties);
	}

	private  void runInParallel(final JCSMPProperties properties) throws JCSMPException {
		final ExecutorService executor = Executors.newFixedThreadPool(MessageConstants.MAX_PARALLEL_THREADS);
		for (int i = 0; i < MessageConstants.MAX_PARALLEL_THREADS; i++) {
			executor.submit(run(properties));
		}
		executor.shutdown();
	}

	private  Runnable run(final JCSMPProperties properties) {
		return new Runnable() {
			@Override
			public void run() {
				try {
					final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
					session.connect();
					configureSession(session);
					final XMLMessageProducer prod = createProducer(session);

					for (int i = 1; i <= MessageConstants.SENDING_COUNT; i++) {
						final Destination queue = rand.getRandomDestination();
						final String text = rand.getRandomMessage();
						final TextMessage msg = createMessage(text, i, queue.getName());
						msg.setPriority(i % 10);
						prod.send(msg, queue);
						messageCount.incrementAndGet();
					}
					session.closeSession();
				} catch (final Exception e) {
					System.out.println(e);
				}
			}
		};
	}

	public abstract void configureSession(JCSMPSession session) throws JCSMPException;

	public abstract  XMLMessageProducer createProducer(final JCSMPSession session) throws JCSMPException;

	public abstract TextMessage createMessage(final String text, final int i, final String destinationName);
}
