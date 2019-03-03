package subscriber;

import java.io.IOException;

public class Init {

    public static void main(String[] args) {
        try {
            //SUBSCRIBER1
            final String SUBSCRIBER1_ADDR = "127.0.0.1";
            int SUBSCRIBER1_PORT = 9000;
            final String ZOOKEEPER_ADDR = "127.0.0.1";
            int ZOOKEEPER_PORT = 8889;
            String TOPIC = "";

            Subscriber subscriber = new Subscriber(SUBSCRIBER1_ADDR, SUBSCRIBER1_PORT, ZOOKEEPER_ADDR, ZOOKEEPER_PORT);
            subscriber.registerTopic(TOPIC);
            System.out.println("Subscriber on " + SUBSCRIBER1_ADDR + ":"+ SUBSCRIBER1_PORT + " register with topic " + TOPIC);
            subscriber.start();

            //SUBSCRIBER2
            //SUBSCRIBER3

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
