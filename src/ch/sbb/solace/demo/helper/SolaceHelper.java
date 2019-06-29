package ch.sbb.solace.demo.helper;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;

public class SolaceHelper {

	private static final int RECEIVE_BUFFER_SIZE = 128_000;
	private static final int SEND_BUFFER_SIZE = 128_000;
	public  static final String TOPIC_MYCLASS_1_0 = "msgsize/direct/json/myclass/1.0";
	public  static final String TOPIC_MYCLASS_2_0 = "msgsize/direct/json/myclass/2.0";
	public  static final String TOPIC_YOURCLASS_1_0 = "msgsize/direct/json/yourclass/1.0";
	public  static final String TOPIC_DEMO = "demo";
	public  static final String TOPIC_IPAD_CON = "ipad_connection_test";

	public static final String TOPIC_PEQ_REP = "reqrep/direct/json/myclass/1.0";

	public static final String QUEUE_NAME = "Q/tutorial";	
	
	public static void setupLogging(Level level) {
		LogManager manager = LogManager.getLogManager();
		Logger rootLogger = manager.getLogger("");
		rootLogger.setLevel(level);
		for (Handler h : rootLogger.getHandlers()) {
			h.setLevel(level);
		}
	}

	public static JCSMPProperties setupProperties() {
		String host = "shared-rcssolace-node01.otc-test.sbb.ch"; // shared-rcssolace-node02.otc-test.sbb.ch
		String vpn = "pingu-VPN";
		String user = "default";
		String password = "default";
		
		if (System.getProperty("host") != null) {
			host = System.getProperty("host");
		}
		
		if (System.getProperty("vpn") != null) {
			vpn = System.getProperty("vpn");
		}
		
		if (System.getProperty("user") != null) {
			user = System.getProperty("user");
		}
		
		if (System.getProperty("password") != null) {
			password = System.getProperty("password");
		}
		
		if (System.getProperty("count") != null) {
			MessageConstants.SENDING_COUNT = Integer.parseInt(System.getProperty("count"));
			if (MessageConstants.SENDING_COUNT < 1) {
				MessageConstants.SENDING_COUNT = Integer.MAX_VALUE;
			}
			
			System.out.println(MessageConstants.SENDING_COUNT);
		}
		
		if (System.getProperty("threads") != null) {
			MessageConstants.PARALLEL_THREADS = Integer.parseInt(System.getProperty("threads"));
		}
		
		System.out.println(String.format("host:%s, user:%s, vpn:%s", host, user, vpn));
		
		final JCSMPProperties properties = new JCSMPProperties();
		properties.setProperty(JCSMPProperties.HOST, host); // host:port
		properties.setProperty(JCSMPProperties.USERNAME, user); // client-username
		properties.setProperty(JCSMPProperties.PASSWORD, password); // client-password
		properties.setProperty(JCSMPProperties.VPN_NAME, vpn); // message-vpn
		properties.setProperty(JCSMPProperties.REAPPLY_SUBSCRIPTIONS, true);
		properties.setProperty(JCSMPProperties.SUB_ACK_WINDOW_SIZE, 255); // Default: 255
		properties.setProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE, 255); // Default:   1 for Java else 255
		
		JCSMPChannelProperties channelProperties = (JCSMPChannelProperties) properties
				.getProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES);
		channelProperties.setSendBuffer(SEND_BUFFER_SIZE);
		channelProperties.setReceiveBuffer(RECEIVE_BUFFER_SIZE);
		channelProperties.setConnectRetries(-1);
		channelProperties.setReconnectRetries(-1);

		return properties;
	}
	
	public static JCSMPSession connect() throws JCSMPException {
		System.out.println("Solace client initializing...");
		final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(setupProperties());
		session.connect();
		
		return session;
	}
}
