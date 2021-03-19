package ru.ermakovis.producer;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class Producer {
    private final static String ROUTENAME = "route";
    private final static String HOSTNAME = "localhost";

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOSTNAME);
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(ROUTENAME, BuiltinExchangeType.TOPIC);
            String input;
            while (!"exit".equals(input = scanner.nextLine())) {
                String[] parts = input.split("\\s+", 2);
                if (parts.length != 2) continue;
                channel.basicPublish(ROUTENAME, parts[0], null, parts[1].getBytes(StandardCharsets.UTF_8));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
