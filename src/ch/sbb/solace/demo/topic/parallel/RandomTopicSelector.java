package ch.sbb.solace.demo.topic.parallel;

import java.util.ArrayList;
import java.util.List;

import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.Topic;

import ch.sbb.solace.demo.parallel.base.RandomSelector;

public class RandomTopicSelector extends RandomSelector {
	private final List<Destination> topics = new ArrayList<>(100);

	public RandomTopicSelector(final int minQueue, final int maxQueue) {
		super(minQueue, maxQueue);
		for (int i = minQueue; i <= maxQueue; i++) {
			final Topic topic = JCSMPFactory.onlyInstance().createTopic("topic/parallel/" + i);
			topics.add(topic);
		}
	}

	@Override
	public Destination getRandomDestination() {
		return topics.get(getRandomIndex());
	}

	@Override
	public List<Destination> getAllDestinations() {
		return new ArrayList<>(topics);
	}

}
