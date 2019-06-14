package ch.sbb.solace.demo.msgsize;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPProperties;

public class SolaceHelper {

	private static final int RECEIVE_BUFFER_SIZE = 128_000;
	private static final int SEND_BUFFER_SIZE = 128_000;
	public static final String TOPIC_MYCLASS_1_0 = "msgsize/direct/json/myclass/1.0";
	public static final String TOPIC_MYCLASS_2_0 = "msgsize/direct/json/myclass/2.0";
	public static final String TOPIC_YOURCLASS_1_0 = "msgsize/direct/json/yourclass/1.0";

	public static void setupLogging(Level level) {
		LogManager manager = LogManager.getLogManager();
		Logger rootLogger = manager.getLogger("");
		rootLogger.setLevel(level);
		for (Handler h : rootLogger.getHandlers()) {
			h.setLevel(level);
		}
	}

	public static JCSMPProperties setupProperties() {
		final String host = "shared-rcssolace-node02.otc-test.sbb.ch";
		final String vpn = "pingu-VPN";
		final String user = "default";
		final String password = "default";

		final JCSMPProperties properties = new JCSMPProperties();
		properties.setProperty(JCSMPProperties.HOST, host); // host:port
		properties.setProperty(JCSMPProperties.USERNAME, user); // client-username
		properties.setProperty(JCSMPProperties.VPN_NAME, vpn); // message-vpn
		properties.setProperty(JCSMPProperties.PASSWORD, password); // client-password

		JCSMPChannelProperties channelProperties = (JCSMPChannelProperties) properties
				.getProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES);
		channelProperties.setSendBuffer(SEND_BUFFER_SIZE);
		channelProperties.setReceiveBuffer(RECEIVE_BUFFER_SIZE);
		return properties;
	}
}
