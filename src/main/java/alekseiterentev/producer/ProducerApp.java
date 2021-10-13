package alekseiterentev.producer;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import static org.apache.commons.lang3.StringUtils.SPACE;
import static org.apache.commons.lang3.StringUtils.substringAfter;
import static org.apache.commons.lang3.StringUtils.substringBefore;

public class ProducerApp {

    private static final String EXCHANGE_NAME = "ItNewsBlock";

    public static void main(String[] args) throws IOException, TimeoutException {
        Scanner sc = new Scanner(System.in);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

            while (true) {
                String command = sc.nextLine();
                String topic = substringBefore(command, SPACE);
                String message = substringAfter(command, SPACE);
                channel.basicPublish(EXCHANGE_NAME, topic, null, message.getBytes("UTF-8"));
                System.out.println("OK");
            }
        }
    }
}
