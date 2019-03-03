package subscriber;

import org.json.simple.JSONObject;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;

import java.util.Set;

public class Subscriber {

    private String subscriberAddr;
    private int subscriberPort;

    private String zookeeperAddr ;
    private int zookeeperPort;

    private Set<String> topicSet = new HashSet<>();

    public Subscriber(String subscriberAddr, int subscriberPort, String zookeeperAddr, int zookeeperPort) {
        this.subscriberAddr = subscriberAddr;
        this.subscriberPort = subscriberPort;
        this.zookeeperAddr = zookeeperAddr;
        this.zookeeperPort = zookeeperPort;

    }

    public void registerTopic(String topic) {
        topicSet = new HashSet<>();
        topicSet.add(topic);

        JSONObject obj = new JSONObject();
        JSONObject topicObj = new JSONObject();

        topicObj.put("topic", topic);

        obj.put("sender", subscriberAddr + subscriberPort);
        obj.put("content", topicObj);

        try {
            Socket socket = new Socket(zookeeperAddr, zookeeperPort);
            OutputStreamWriter oos = new OutputStreamWriter(socket.getOutputStream());
            oos.write(obj.toJSONString());
            InputStreamReader ois = new InputStreamReader(socket.getInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //从zookeeper知道有哪些topic可以用
    //又一个
    private void getTopic() {

    }

    public void start() throws IOException {
        ServerSocket serverSocket = new ServerSocket(subscriberPort);
        while (true){
            Socket socket = serverSocket.accept();
            SubscriberThread subscriberThread = new SubscriberThread(socket);
            subscriberThread.start();
        }
    }
}

