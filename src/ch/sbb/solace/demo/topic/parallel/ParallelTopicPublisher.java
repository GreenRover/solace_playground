package ch.sbb.solace.demo.topic.parallel;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.User_Cos;
import com.solacesystems.jcsmp.XMLMessageProducer;

import ch.sbb.solace.demo.helper.MessagePayloadHelper;
import ch.sbb.solace.demo.parallel.base.ParallelSender;
import ch.sbb.solace.demo.parallel.base.RandomSelector;

/**
 * Options:
 *              
 * -Dhost         Auth credentials
 * -Dvpn
 * -Duser
 * -Dpassword
 * 
 * -Dcount=10_000  Number of msg to send  0==MaxInt
 * -Dthreads=12	   Number of threads to use to send
 * -DminQueue=1    Topics to send data to
 * -DmaxQueue=50   Topics to send data to
 * -DmsgSize=0     The size of the msg to send in byte. 0 means random changing between 10b - 2000b
 */
public class ParallelTopicPublisher extends ParallelSender {

	private static final RandomSelector rand = new RandomTopicSelector( //
			Integer.parseInt(System.getProperty("minQueue", "1")), //
			Integer.parseInt(System.getProperty("maxQueue", "50")) //
	);

	public ParallelTopicPublisher() {
		super("ParallelTopicPublisher", rand);

		// createMessage is expensive. And because topic dont have ack, we can re use
		// it.
//		msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
	}

	@Override
	public XMLMessageProducer createProducer(final JCSMPSession session) throws JCSMPException {
		final XMLMessageProducer prod = session.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
			@Override
			public void responseReceived(final String messageID) {
				System.out.println("Producer received response for msg: " + messageID);
			}

			@Override
			public void handleError(final String messageID, final JCSMPException e, final long timestamp) {
				System.out.printf("Producer received error for msg: %s@%s - %s%n", messageID, timestamp, e);
			}
		});
		return prod;
	}

	@Override
	public TextMessage createMessage(final String text, final int i, final String topicName) {
		TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		final String payload = MessagePayloadHelper.createPayload(text, i, topicName);
		msg.setText(payload);
		int prio = i % 10;
		msg.setPriority(prio);
		msg.setCos((prio > 7) ? User_Cos.USER_COS_3 : User_Cos.USER_COS_1);
		return msg;
	}

	@Override
	public void configureSession(final JCSMPSession session) throws JCSMPException {
	}

	public static void main(final String[] args) throws Exception {
		new ParallelTopicPublisher().go();
	}
}
