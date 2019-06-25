package ch.sbb.solace.demo.parallel.base;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.solacesystems.jcsmp.Destination;

import ch.sbb.solace.demo.helper.MessageConstants;

public abstract class RandomSelector {

	private final List<String> messages = new ArrayList<>();
	protected final Random rand = new Random();

	public RandomSelector() {
		messages.add(MessageConstants.MESSAGE_B10);
		messages.add(MessageConstants.MESSAGE_B20);
		messages.add(MessageConstants.MESSAGE_B50);
		messages.add(MessageConstants.MESSAGE_B100);
		messages.add(MessageConstants.MESSAGE_B200);
		messages.add(MessageConstants.MESSAGE_B500);
		messages.add(MessageConstants.MESSAGE_K1);
		messages.add(MessageConstants.MESSAGE_K2);
	}

	public abstract Destination getRandomDestination();

	public abstract List<Destination> getAllDestinations();

	public String getRandomMessage() {
		final int index = rand.nextInt(messages.size());
		return messages.get(index);
	}
}
