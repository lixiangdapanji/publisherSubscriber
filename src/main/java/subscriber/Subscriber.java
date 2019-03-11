package subscriber;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

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


    /**
     *
     */
    public void registerTopic() {

        List<String> keysAsArray = new ArrayList<>(topicMap.keySet());
        Random r = new Random();
        String topic = keysAsArray.get(r.nextInt(keysAsArray.size()));

        JSONObject obj = new JSONObject();
        JSONObject topicObj = new JSONObject();

        topicObj.put("topic", topic);

        obj.put("sender", subscriberAddr + ":" + subscriberPort);
        obj.put("action", "CLIENT_REGISTER");
        obj.put("content", topicObj);

        try {
            Socket socket = new Socket(zookeeperAddr, zookeeperPort);
            //send request
            PrintStream out = new PrintStream(socket.getOutputStream());
            out.println(obj.toJSONString());
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
            JSONObject request = new JSONObject();
            request.put("sender", subscriberAddr + ":" + subscriberPort);
            request.put("action", "GET_TOPIC");
            request.put("content", new JSONObject());
            PrintStream out = new PrintStream(socket.getOutputStream());
            out.println(request);

            //get response
            //InputStreamReader ins = new InputStreamReader(socket.getInputStream());
            BufferedReader ins = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String in = ins.readLine();
            JSONParser parser = new JSONParser();
            JSONObject response = (JSONObject) parser.parse(in);
            String content = (String) response.get("content");
            String[] topics = content.split(",");
            for (String s : topics) {
                topicMap.put(s, new AtomicInteger(0));
            }
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
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
                JSONParser parser = new JSONParser();
                JSONObject jsonObject = (JSONObject) parser.parse(in);
                String action = (String) jsonObject.get("action");

                if (action.equals("SEND_MESSAGE")) {
                    JSONObject content = (JSONObject) jsonObject.get("content");
                    String sender = (String) jsonObject.get("sender");
                    String[] serverID = sender.split(":");
                    String topic = (String) content.get("topic");
                    String message = (String) content.get("msg");
                    int id = Integer.valueOf((String) content.get("id"));

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
//                        JSONObject request = new JSONObject();
//                        request.put("sender", subscriberAddr + ":" + subscriberPort);
//                        request.put("action", "MISSING_MESSAGE");
//                        request.put("content",  wrongId + ":" + (id - 1) );
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
            } catch (ParseException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

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

            subscriber1.registerTopic();
            System.out.println("Subscriber on " + SUBSCRIBER1_ADDR + ":" + SUBSCRIBER1_PORT + " register with topic " + TOPIC);
            subscriber1.start();

            //SUBSCRIBER2
            //SUBSCRIBER3

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}