package ch.sbb.solace.demo.topic.parallel;

import java.util.ArrayList;
import java.util.List;

import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.Topic;

import ch.sbb.solace.demo.parallel.base.RandomSelector;

public class RandomTopicSelector extends RandomSelector {
	private static final int MAX_TOPICS = 100;

	private final List<Destination> topics = new ArrayList<>(MAX_TOPICS);

	public RandomTopicSelector() {
		super();
		for (int i = 0; i < MAX_TOPICS; i++) {
			final Topic topic = JCSMPFactory.onlyInstance().createTopic("topic/parallel/" + i);
			topics.add(topic);
		}
	}

	@Override
	public Destination getRandomDestination() {
		final int index = rand.nextInt(MAX_TOPICS);
		return topics.get(index);
	}

	@Override
	public List<Destination> getAllDestinations() {
		return new ArrayList<>(topics);
	}

}
