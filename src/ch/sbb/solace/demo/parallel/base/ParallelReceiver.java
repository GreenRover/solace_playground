package ch.sbb.solace.demo.parallel.base;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import com.solacesystems.jcsmp.Consumer;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;

import ch.sbb.solace.demo.helper.ListSplitter;
import ch.sbb.solace.demo.helper.MessageConstants;
import ch.sbb.solace.demo.helper.SolaceHelper;

public abstract class ParallelReceiver {

	private final String name;
	private final RandomSelector rand;

	public static AtomicInteger messageCountPerSecond = new AtomicInteger(0);
	public static AtomicInteger messageCount = new AtomicInteger(0);
	protected int MAX_MESSAGES = MessageConstants.PARALLEL_THREADS * MessageConstants.SENDING_COUNT;

	// statistics about priorities of received messages
	public static Map<Integer, Integer> map = new ConcurrentHashMap<>();

	public ParallelReceiver(final String name, final RandomSelector rand) {
		this.name = name;
		this.rand = rand;
	}

	public void go() throws JCSMPException {
		SolaceHelper.setupLogging(Level.WARNING);
		System.out.printf("%s initializing...%n", name);
		final JCSMPProperties properties = SolaceHelper.setupProperties();

		final List<List<Destination>> destinations = ListSplitter.chunk(rand.getAllDestinations(),
				MessageConstants.PARALLEL_THREADS);

		final ExecutorService executor = Executors.newFixedThreadPool(MessageConstants.PARALLEL_THREADS);
		for (int i = 0; i < destinations.size(); i++) {
			final List<Destination> destChunk = destinations.get(i);
			executor.submit(() -> {
				try {
					runInThread(properties, destChunk);
				} catch (final JCSMPException | InterruptedException e) {
					System.out.println(e);
					e.printStackTrace();
				}
			});
		}
		executor.shutdown();

		final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
		executorService.scheduleAtFixedRate(() -> {
			final int countPerSecond = messageCountPerSecond.getAndSet(0);
			final int countTotal = messageCount.get();
			System.out.printf("%,d msg/s [%,d | %,d]%n", countPerSecond, countTotal, MAX_MESSAGES);
		}, 0, 1, TimeUnit.SECONDS);

		executorService.scheduleAtFixedRate(() -> {
			System.out.printf("  message prio stats: %s%n", calculateMapStatistics(map));
		}, 0, 30, TimeUnit.SECONDS);
	}

	private void runInThread(final JCSMPProperties properties, final List<Destination> destinations)
			throws JCSMPException, InterruptedException {
		final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
		session.connect();

		System.out.println(Thread.currentThread().getName() + " consuming " + destinations.size() + " destinations");
		Thread.sleep(10);
		for (final Destination dest : destinations) {
			configureSession(session, dest);
			final Consumer con = createReceiver(session, dest);
			con.start();
		}
	}

	private String calculateMapStatistics(final Map<Integer, Integer> m) {
		final StringBuilder sb = new StringBuilder();
		final int sum = map.values().stream().mapToInt(Integer::intValue).sum();
		for (final Entry<Integer, Integer> el : m.entrySet()) {
			final int k = Integer.valueOf(el.getKey());
			final int v = Integer.valueOf(el.getValue());
			final float percent = ((float) v / sum) * 100.0f;
			sb.append(k).append("=").append(v).append(" ").append(Math.round(percent)).append("%, ");
		}
		return sb.toString();
	}

	public abstract void configureSession(JCSMPSession session, Destination destination) throws JCSMPException;

	public abstract Consumer createReceiver(JCSMPSession session, Destination destination) throws JCSMPException;
}
