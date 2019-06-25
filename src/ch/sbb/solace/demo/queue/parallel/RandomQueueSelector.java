package ch.sbb.solace.demo.queue.parallel;

import java.util.ArrayList;
import java.util.List;

import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.Queue;

import ch.sbb.solace.demo.parallel.base.RandomSelector;

public class RandomQueueSelector extends RandomSelector {
	private static final int MAX_QUEUES = 50;
	
	private final List<Destination> queues = new ArrayList<>(MAX_QUEUES);

	public RandomQueueSelector() {
		super();
		for (int i = 0; i < MAX_QUEUES; i++) {
			final Queue queue = JCSMPFactory.onlyInstance().createQueue("queue/parallel/" + i);
			queues.add(queue);
		}
	}

	@Override
	public Destination getRandomDestination() {
		final int index = rand.nextInt(MAX_QUEUES);
		return queues.get(index);
	}

	@Override
	public List<Destination> getAllDestinations() {
		return new ArrayList<>(queues);
	}

}
