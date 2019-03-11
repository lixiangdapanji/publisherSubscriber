package subscriber;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

/**
 *
 */
public class Subscriber {

    private String subscriberAddr;
    private int subscriberPort;

    private String zookeeperAddr;
    private int zookeeperPort;

    private Map<String, Integer> topicMap;

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
    public void registerTopic(String topic) {

//        List<String> keysAsArray = new ArrayList<>(topicMap.keySet());
//        Random r = new Random();
//        String topic = keysAsArray.get(r.nextInt(keysAsArray.size()));

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
                topicMap.put(s, -1);
            }
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
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
        while (true) {
            Socket socket = serverSocket.accept();
            SubscriberThread subscriberThread = new SubscriberThread(socket);
            subscriberThread.start();
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
                String action = (String)jsonObject.get("action");
                System.out.println("Action: " + action);
                if (action.equals("SEND_MESSAGE")) {
                    JSONObject content = (JSONObject) jsonObject.get("content");
                    String sender = (String) jsonObject.get("sender");
                    String[] serverID = sender.split(":");
                    String topic = (String) content.get("topic");
                    String message = (String) content.get("msg");
                    int id = Integer.valueOf((String)content.get("id"));
                    if(topicMap.get(topic) == -1){
                        topicMap.put(topic, (int)id);
                    }else{
                        topicMap.put(topic, topicMap.getOrDefault(topic, 0) + 1);
                    }
                    int wrongId = topicMap.get(topic);
                    if (wrongId < id) {
                        //notify server message missing
                        JSONObject request = new JSONObject();
                        request.put("sender", subscriberAddr + ":" + subscriberPort);
                        request.put("action", "MISSING_MESSAGE");
                        request.put("content",  wrongId + ":" + (id - 1) );
                        topicMap.put(topic, id);
                        try {
                            Socket socket = new Socket(serverID[0], Integer.valueOf(serverID[1]));
                            //send request
                            PrintStream out = new PrintStream(socket.getOutputStream());
                            out.println(request);

                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    System.out.println(id + ": " + message);
                    // delete client once the id == random, delete client message received should be in 20 - 300
                    Random r = new Random();
                    //int target = 20 + r.nextInt(300);
                    int target = 10;
                    if (id == target) {
                        JSONObject request = new JSONObject();
                        request.put("sender", subscriberAddr + ":" + subscriberPort);
                        request.put("action", "DELETE_CLIENT");
                        request.put("content",  wrongId + ":" + (id - 1) );
                        topicMap.put(topic, id);
                        try {
                            Socket socket = new Socket(zookeeperAddr, zookeeperPort);
                            //send request
                            PrintStream out = new PrintStream(socket.getOutputStream());
                            out.println(request);

                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }

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