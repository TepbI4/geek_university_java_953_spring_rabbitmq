package alekseiterentev.receiver;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import static org.apache.commons.lang3.StringUtils.SPACE;
import static org.apache.commons.lang3.StringUtils.substringAfter;
import static org.apache.commons.lang3.StringUtils.substringBefore;

public class ReceiverApp {

    public static final String EXCHANGE_NAME = "ItNewsBlock";
    public static final String SET_TOPIC = "set_topic";
    public static final String UNSET_TOPIC = "unset_topic";

    public static void main(String[] args) throws IOException, TimeoutException {
        Scanner sc = new Scanner(System.in);

        String input;

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        String queueName = channel.queueDeclare().getQueue();
        System.out.println("My queue name: " + queueName);

        System.out.println(" [*] Waiting for messages");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + message + "'");
            System.out.println(Thread.currentThread().getName());
        };

        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });

        while (true) {
            input = sc.nextLine();
            if (SET_TOPIC.equals(substringBefore(input, SPACE))) {
                channel.queueBind(queueName, EXCHANGE_NAME, substringAfter(input, SPACE));
            }

            if (UNSET_TOPIC.equals(substringBefore(input, SPACE))) {
                channel.queueUnbind(queueName, EXCHANGE_NAME, substringAfter(input, SPACE));
            }
        }
    }
}
