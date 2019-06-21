package ch.sbb.solace.demo.helper;

public class MessageConstants {

	public static  int SENDING_COUNT = 10_00_000;
	public static final int MAX_PARALLEL_THREADS = 12;
	public static final int REQUEST_TIMEOUT_IN_MILLIS = 10_000;
	
	public static final int MAX_MESSAGES_IN_QUEUE = 1_000;

	public enum DataType {
		K1_TextMessage, K10_TextMessage, K100_TextMessage, K1000_TextMessage
	}

	public static String MESSAGE_B10 = createStringOfSize(10);
	public static String MESSAGE_B20 = createStringOfSize(20);
	public static String MESSAGE_B50 = createStringOfSize(20);
	public static String MESSAGE_B100 = createStringOfSize(100);
	public static String MESSAGE_B200 = createStringOfSize(200);
	public static String MESSAGE_B500 = createStringOfSize(500);

	public static String MESSAGE_K1 = createStringOfSize(1000);
	public static String MESSAGE_K2 = createStringOfSize(2000);
	public static String MESSAGE_K5 = createStringOfSize(5000);
	public static String MESSAGE_K10 = createStringOfSize(10_000);
	
	public static String MESSAGE_K100 = createStringOfSize(100_000);
	public static String MESSAGE_K1000 = createStringOfSize(1_000_000);

	private static String createStringOfSize(int n) {
		StringBuilder outputBuffer = new StringBuilder(n);
		for (int i = 0; i < n; i++) {
			outputBuffer.append("x");
		}
		return outputBuffer.toString();
	}

}
