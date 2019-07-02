package ch.sbb.solace.demo.helper;

public class MessageConstants {

	public static int SENDING_COUNT = 10_000;
	public static int PARALLEL_THREADS = 12;
	
	public static final int REQUEST_TIMEOUT_IN_MILLIS = 10_000;
	
	public static final int MAX_MESSAGES_IN_QUEUE = 1_000;

	public static String createStringOfSize(int n) {
		StringBuilder outputBuffer = new StringBuilder(n);
		for (int i = 0; i < n; i++) {
			outputBuffer.append("x");
		}
		return outputBuffer.toString();
	}

}
