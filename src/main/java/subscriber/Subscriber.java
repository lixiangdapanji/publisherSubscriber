package subscriber;


import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class Subscriber {

    private String subscriberAddr;
    private int subscriberPort;

    private String zookeeperAddr;
    private int zookeeperPort;

    private Map<String, AtomicInteger> topicMap;

    /**
     * @param subscriberAddr
     * @param subscriberPort
     * @param zookeeperAddr
     * @param zookeeperPort
     */
    public Subscriber(String subscriberAddr, int subscriberPort, String zookeeperAddr, int zookeeperPort) {
        this.subscriberAddr = subscriberAddr;
        this.subscriberPort = subscriberPort;
        this.zookeeperAddr = zookeeperAddr;
        this.zookeeperPort = zookeeperPort;
        topicMap = new HashMap<>();
    }


    public void registerTopic(String topic) {

//        List<String> keysAsArray = new ArrayList<>(topicMap.keySet());
//        Random r = new Random();
//        String topic = keysAsArray.get(r.nextInt(keysAsArray.size()));

        JsonObject obj = new JsonObject();
        JsonObject topicObj = new JsonObject();

        topicObj.addProperty("topic", topic);

        obj.addProperty("sender", subscriberAddr + ":" + subscriberPort);
        obj.addProperty("action", "CLIENT_REGISTER");
        obj.add("content", topicObj);

        try {
            Socket socket = new Socket(zookeeperAddr, zookeeperPort);
            //send request
            PrintStream out = new PrintStream(socket.getOutputStream());
            out.println(obj.toString());
            //socket.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     *
     */
    //从zookeeper知道有哪些topic可以用
    //又一个
    protected void getTopic() {
        try {
            Socket socket = new Socket(zookeeperAddr, zookeeperPort);
            //send request
            JsonObject request = new JsonObject();
            request.addProperty("sender", subscriberAddr + ":" + subscriberPort);
            request.addProperty("action", "GET_TOPIC");
            request.add("content", new JsonObject());
            PrintStream out = new PrintStream(socket.getOutputStream());
            out.println(request);

            //get response
            //InputStreamReader ins = new InputStreamReader(socket.getInputStream());
            BufferedReader ins = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String in = ins.readLine();
            JsonParser parser = new JsonParser();
            JsonObject response = parser.parse(in).getAsJsonObject();
            String content = response.get("content").getAsString();
            String[] topics = content.split(",");
            for (String s : topics) {
                topicMap.put(s, new AtomicInteger(0));
            }
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        StringBuilder sb = new StringBuilder();
        System.out.print("Select the topic to subscribe: ");
        sb.append("{");
        for (String topic : topicMap.keySet()) {
            sb.append(topic);
            sb.append(", ");
        }
        sb.delete(sb.length() - 2, sb.length());
        sb.append("}");
        System.out.println(sb.toString());
    }

    /**
     * @throws IOException
     */
    public void start() throws IOException {
        ServerSocket serverSocket = new ServerSocket(subscriberPort);

        ExecutorService threadPool = Executors.newFixedThreadPool(10);
        while (true) {
            Socket socket = serverSocket.accept();
            SubscriberThread subscriberThread = new SubscriberThread(socket);
            threadPool.execute(subscriberThread);
        }
    }

    class SubscriberThread extends Thread {
        private Socket socket;

        public SubscriberThread(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try {
                BufferedReader ins = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String in = ins.readLine();
                JsonParser parser = new JsonParser();
                JsonObject jsonObject = parser.parse(in).getAsJsonObject();
                String action = jsonObject.get("action").getAsString();

                if (action.equals("SEND_MESSAGE")) {
                    JsonObject content = jsonObject.get("content").getAsJsonObject();
                    String sender = jsonObject.get("sender").getAsString();
                    String[] serverID = sender.split(":");
                    String topic =  content.get("topic").getAsString();
                    String message = content.get("msg").getAsString();
                    int id = Integer.valueOf(content.get("id").getAsString());

                    int curCount = topicMap.get(topic).get();
                    if (curCount == 0) {
                        topicMap.get(topic).set(id);
                    } else {
                        if (curCount != id - 1) {
                            System.out.println("missing msg topic: " + topic + " from " + (curCount + 1) + " to " + (id - 1));
                        }
                        topicMap.get(topic).set(id);
                    }

//                    int wrongId = topicMap.get(topic);
//                    if (wrongId < id) {
//                        //notify server message missing
//                        JsonObject request = new JsonObject();
//                        request.addProperty("sender", subscriberAddr + ":" + subscriberPort);
//                        request.addProperty("action", "MISSING_MESSAGE");
//                        request.addProperty("content",  wrongId + ":" + (id - 1) );
//                        topicMap.put(topic, id);
//                        try {
//                            Socket socket = new Socket(serverID[0], Integer.valueOf(serverID[1]));
//                            //send request
//                            PrintStream out = new PrintStream(socket.getOutputStream());
//                            out.println(request);
//
//                        } catch (IOException e) {
//                            e.printStackTrace();
//                        }
//                    }
                    System.out.println(topic + " " + id + ": " + message);
                }

                ins.close();
                socket.close();
            }catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        try {
            final String SUBSCRIBER_ADDR = "127.0.0.1";
            Scanner scanner = new Scanner(System.in);
            System.out.print("Please input port number: ");
            int SUBSCRIBER_PORT = Integer.valueOf(scanner.nextLine());
            final String ZOOKEEPER_ADDR = "127.0.0.1";
            int ZOOKEEPER_PORT = 8889;

            Subscriber subscriber1 = new Subscriber(SUBSCRIBER_ADDR, SUBSCRIBER_PORT, ZOOKEEPER_ADDR, ZOOKEEPER_PORT);
            subscriber1.getTopic();
            String topic = scanner.nextLine();
            subscriber1.registerTopic(topic);
            System.out.println("Subscriber on " + SUBSCRIBER_ADDR + ":"+ SUBSCRIBER_PORT + " register with topic " + topic);
            subscriber1.start();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}