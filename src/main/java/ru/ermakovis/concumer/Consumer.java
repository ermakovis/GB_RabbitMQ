package ru.ermakovis.concumer;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

//Может использовать Direct вместо Topic, но непонятно зачем
//Топик делает все тоже самое и чуть больше
public class Consumer {
    private final static Logger logger =
            LoggerFactory.getLogger(Consumer.class);
    private final static String ROUTENAME = "route";
    private final static String HOSTNAME = "localhost";

    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOSTNAME);

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        String queueName = channel.queueDeclare().getQueue();
        channel.exchangeDeclare(ROUTENAME, BuiltinExchangeType.TOPIC);

        DeliverCallback callback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };
        channel.basicConsume(queueName, true, callback, consumerTag -> {
        });

        String input;
        while (!"exit".equals(input = scanner.nextLine())) {
            String[] parts = input.split("\\s+", 2);

            if (parts.length != 2) {
                logger.warn("Incorrect command");
            } else if ("subscribe".equals(parts[0])) {
                channel.queueBind(queueName, ROUTENAME, parts[1]);
                logger.info("Queue bound " + parts[1]);
            } else if ("unsubscribe".equals(parts[0])) {
                channel.queueUnbind(queueName, ROUTENAME, parts[1]);
                logger.info("Queue unbound " + parts[1]);
            } else {
                logger.error("Unknown command " + parts[0]);
            }
        }
        channel.close();
        connection.close();
    }
}
