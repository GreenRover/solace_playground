package ch.sbb.solace.demo;

import com.solacesystems.jcsmp.InvalidPropertiesException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;

public class ConnectionFactory {
	final static String HOST = "shared-rcssolace-node01.otc-test.sbb.ch";
	final static String VPN = "pingu-VPN";
	final static String USER = "default";
	final static String PASSWORD = "default";
	
	public static JCSMPProperties getCredentials() {
        final JCSMPProperties properties = new JCSMPProperties();
        properties.setProperty(JCSMPProperties.HOST, HOST);      // msg-backbone ip:port
        properties.setProperty(JCSMPProperties.VPN_NAME, VPN);  // message-vpn
        properties.setProperty(JCSMPProperties.USERNAME, USER); 
        properties.setProperty(JCSMPProperties.PASSWORD, PASSWORD); 
        
        return properties;
	}
	
	public static JCSMPSession getSession() throws InvalidPropertiesException {
		return JCSMPFactory.onlyInstance().createSession(getCredentials());
	}
}
