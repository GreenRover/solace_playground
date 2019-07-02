package ch.sbb.solace.demo.parallel.base;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.solacesystems.jcsmp.Destination;

import ch.sbb.solace.demo.helper.MessageConstants;

public abstract class RandomSelector {

	private final List<String> messages = new ArrayList<>();
	private final Map<Integer, String> messageCache = new HashMap<>();
	protected final Random rand = new Random();
	protected int minQueue;
	protected int maxQueue;
	
	public RandomSelector(final int minQueue, final int maxQueue) {
		this.minQueue = minQueue;
		this.maxQueue = maxQueue;
		
		messages.add(MessageConstants.createStringOfSize(10));
		messages.add(MessageConstants.createStringOfSize(20));
		messages.add(MessageConstants.createStringOfSize(50));
		messages.add(MessageConstants.createStringOfSize(100));
		messages.add(MessageConstants.createStringOfSize(200));
		messages.add(MessageConstants.createStringOfSize(500));
		messages.add(MessageConstants.createStringOfSize(1000));
		messages.add(MessageConstants.createStringOfSize(2000));
	}

	public abstract Destination getRandomDestination();

	public abstract List<Destination> getAllDestinations();
	
	public int getRandomIndex() {
		return minQueue + rand.nextInt(maxQueue - minQueue);
	}
	
	public String createMessage(final int msgSize) {
		if (msgSize < 1) {
			final int index = rand.nextInt(messages.size());
			return messages.get(index);
		} else {
			return messageCache.computeIfAbsent(msgSize, s -> MessageConstants.createStringOfSize(s));
		}
	}
}
