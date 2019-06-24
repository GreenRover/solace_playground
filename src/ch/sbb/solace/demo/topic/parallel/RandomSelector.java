package ch.sbb.solace.demo.topic.parallel;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.Topic;

import ch.sbb.solace.demo.helper.MessageConstants;

public class RandomSelector {
	private static final int MAX_TOPICS = 1000;
	private final Random rand = new Random();

	private final List<Topic> topics = new ArrayList<>(MAX_TOPICS);
	private final List<String> messages = new ArrayList<>();

	public RandomSelector() {
		for (int i = 0; i < MAX_TOPICS; i++) {
			final Topic topic = JCSMPFactory.onlyInstance().createTopic("topic/parallel/" + i);
			topics.add(topic);
		}

		messages.add(MessageConstants.MESSAGE_B10);
		messages.add(MessageConstants.MESSAGE_B20);
		messages.add(MessageConstants.MESSAGE_B50);
		messages.add(MessageConstants.MESSAGE_B100);
		messages.add(MessageConstants.MESSAGE_B200);
		messages.add(MessageConstants.MESSAGE_B500);
		messages.add(MessageConstants.MESSAGE_K1);
		messages.add(MessageConstants.MESSAGE_K2);
	}

	public Topic getRandomTopic() {
		final int index = rand.nextInt(MAX_TOPICS);
		return topics.get(index);
	}

	public List<Topic> getAllTopics() {
		return new ArrayList<>(topics);
	}

	public String getRandomMessage() {
		final int index = rand.nextInt(messages.size());
		return messages.get(index);
	}
}
