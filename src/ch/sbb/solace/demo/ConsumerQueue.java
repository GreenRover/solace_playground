package ch.sbb.solace.demo;

import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessageListener;

public class ConsumerQueue {

	final int count = 5;
	final CountDownLatch latch = new CountDownLatch(count); // used for synchronizing b/w threads
	private  static int rx_msg_count = 0;

	public static void main(String[] args) throws JCSMPException, InterruptedException {
		new ConsumerQueue();
	}

	public ConsumerQueue() throws JCSMPException, InterruptedException {
		final String queueName = "test/queue";

		System.out.println("HelloWorldConsumer initializing... using: " + Charset.defaultCharset().displayName());

		// Create a JCSMP Session
		final JCSMPSession session = ConnectionFactory.getSession();
		
		final Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
		
//		/*
//		 * Provision a new Queue on the appliance, ignoring if it already
//		 * exists. Set permissions, access type, and provisioning flags.
//		 */
//		EndpointProperties ep_provision = new EndpointProperties();
//		// Set permissions to allow all
//		ep_provision.setPermission(EndpointProperties.PERMISSION_DELETE);
//		// Set access type to exclusive
//		ep_provision.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
//		// Quota to 100MB
//		ep_provision.setQuota(100);
//		// Set queue to respect message TTL
//		ep_provision.setRespectsMsgTTL(Boolean.TRUE);
//		
//		session.provision(queue, ep_provision, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);

		rx_msg_count = 0;
		ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
		flow_prop.setEndpoint(queue);
		FlowReceiver cons = session.createFlow(new MessageDumpListener(), flow_prop);
		cons.start();
		Thread.sleep(10_000);
		System.out.printf("Finished consuming messages, the number of message on the queue is '%s'.\n", rx_msg_count);
		cons.close();

		System.out.println("Exiting.");
		session.closeSession();
	}

	static class MessageDumpListener implements XMLMessageListener {
		public void onException(JCSMPException exception) {
			exception.printStackTrace();
		}

		public void onReceive(BytesXMLMessage message) {
			if (message instanceof TextMessage) {
				System.out.println(((TextMessage)message).getText());
			} else {
				System.out.println("\n======== Received message ======== \n" + message.dump());
			}
			rx_msg_count++;
		}
	}
}
