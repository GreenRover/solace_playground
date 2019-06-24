package ch.sbb.solace.demo.queue.parallel;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.Queue;

import ch.sbb.solace.demo.helper.MessageConstants;

public class RandomSelector {
	private static final int MAX_QUEUES = 50;
	private final Random rand = new Random();

	private final List<Queue> queues = new ArrayList<>(MAX_QUEUES);
	private final List<String> messages = new ArrayList<>();

	public RandomSelector() {
		for (int i = 0; i < MAX_QUEUES; i++) {
			final Queue queue = JCSMPFactory.onlyInstance().createQueue("queue/parallel/" + i);
			queues.add(queue);
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

	public Queue getRandomQueue() {
		final int index = rand.nextInt(MAX_QUEUES);
		return queues.get(index);
	}

	public List<Queue> getAllQueues() {
		return new ArrayList<>(queues);
	}

	public String getRandomMessage() {
		final int index = rand.nextInt(messages.size());
		return messages.get(index);
	}
}
