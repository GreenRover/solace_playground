package ch.sbb.solace.demo.queue.parallel;

import java.util.ArrayList;
import java.util.List;

import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.Queue;

import ch.sbb.solace.demo.parallel.base.RandomSelector;

public class RandomQueueSelector extends RandomSelector {
	private final List<Destination> queues = new ArrayList<>(100);

	public RandomQueueSelector(final int minQueue, final int maxQueue) {
		super(minQueue, maxQueue);
		
		for (int i = minQueue; i <= maxQueue; i++) {
			final Queue queue = JCSMPFactory.onlyInstance().createQueue("queue/parallel/" + i);
			queues.add(queue);
		}
	}

	@Override
	public Destination getRandomDestination() {
		return queues.get(getRandomIndex());
	}

	@Override
	public List<Destination> getAllDestinations() {
		return new ArrayList<>(queues);
	}

}
