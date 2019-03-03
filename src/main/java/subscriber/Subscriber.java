package subscriber;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;

public class Subscriber {

    private String subscriberAddr = "127.0.0.1";
    private int subscriberPort;

    private String zookeeperAddr = "127.0.0.1";
    private int zookeeperPort = 7000;

    private String TOPIC = "";

    private Set<String> TopicSet = new HashSet<>();

    public Subscriber(String subscriberAddr, int subscriberPort, String zookeeperAddr, int zookeeperPort) {
        this.subscriberAddr = subscriberAddr;
        this.subscriberPort = subscriberPort;
        this.zookeeperAddr = zookeeperAddr;
        this.zookeeperPort = zookeeperPort;

    }

    public void registerTopic(String TOPIC) {
        //create a message and
    }

    private void getTopic() {

    }

    public void start() throws IOException {
        ServerSocket serverSocket = new ServerSocket(subscriberPort);
        while (true){
            Socket socket = serverSocket.accept();
            ClientThread clientThread = new ClientThread(socket);
            clientThread.start();
        }
    }

    @Override
    public String toString() {
        return "Subscriber [subscriber address = " + subscriberAddr + ", port = " + subscriberPort + "]";
    }



}

