package subscriber;

import java.io.IOException;
import java.util.Scanner;

public class Init {

    public static void main(String[] args) {
        try {
            //SUBSCRIBER1
            final String SUBSCRIBER1_ADDR = "127.0.0.1";
            Scanner scanner = new Scanner(System.in);
            System.out.print("Please input port number: ");
            int SUBSCRIBER1_PORT = scanner.nextInt();
            final String ZOOKEEPER_ADDR = "127.0.0.1";
            int ZOOKEEPER_PORT = 8889;
            String TOPIC = "";

            Subscriber subscriber1 = new Subscriber(SUBSCRIBER1_ADDR, SUBSCRIBER1_PORT, ZOOKEEPER_ADDR, ZOOKEEPER_PORT);
            subscriber1.getTopic();
            subscriber1.registerTopic("Topic_1");
            System.out.println("Subscriber on " + SUBSCRIBER1_ADDR + ":"+ SUBSCRIBER1_PORT + " register with topic " + TOPIC);
            subscriber1.start();

            //SUBSCRIBER2
            //SUBSCRIBER3

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
