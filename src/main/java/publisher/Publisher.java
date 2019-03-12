package publisher;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.*;
import java.net.Socket;
import java.util.*;

public class Publisher extends Thread {
    private final String zookeeper_ip = "127.0.0.1";
    private int zookeeper_port = 8889;
    private Map<String, String> topicBrokerMap = new HashMap<>();
    private List<String> topicList = new ArrayList<>();

    public Publisher() {
        //initial topic list
        //topicList.add("Topic_1");
    }

    private JsonObject CreateMsg() {
        /*
        message format { sender: publisher,
                         action: NEW_FEED
                         content: { topic: topic,
                                    msg: content }
                        }
         */
        int size = topicList.size();

        JsonObject msg = new JsonObject();
        msg.addProperty("action", "NEW_FEED");

        JsonObject content = new JsonObject();
        content.addProperty("topic", topicList.get((int) (Math.random() * size)));
        content.addProperty("msg", "a new message");

        msg.add("content", content);

        return msg;
    }

    private String getServerAddr(String topic) {
        Socket client = null;
        BufferedReader reader = null;
        String brokerAddr = null;
        PrintStream writer = null;

        JsonObject sent = new JsonObject();
        sent.addProperty("action", "NEW_TOPIC");
        sent.addProperty("content", topic);

        try {
            client = new Socket(zookeeper_ip, zookeeper_port);
            writer = new PrintStream(client.getOutputStream());
            writer.println(sent.toString());
            /*
            received message { sender: zookeeper,
                               topic: topic,
                               content: broker addr }
             */
            reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
            String inputLine = reader.readLine();

            JsonParser parser = new JsonParser();
            JsonObject json = parser.parse(inputLine).getAsJsonObject();
            brokerAddr = json.get("content").getAsString();

            writer.close();
            reader.close();
            client.close();
        } catch (Exception e) {
            System.out.println("Exception in getServerAddr.");
            //e.printStackTrace();
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        }

        return brokerAddr;
    }

    private void sendMsg(String brokerAddr, String topic, JsonObject msg) {
        Socket client = null;
        while (true) {

            try {
                //parse brokerAddr ip:port
                int i = brokerAddr.indexOf(":");
                String ip = brokerAddr.substring(0, i);
                int port = Integer.parseInt(brokerAddr.substring(i + 1));

                System.out.println("sending message to " + ip + ":" + port);
                client = new Socket(ip, port);
                PrintStream writer = new PrintStream(client.getOutputStream());
                writer.println(msg.toString());

                //client.close();
                break;
            } catch (Exception e) {
                System.out.println("Exception in sendMsg.");
                //e.printStackTrace();
                JsonObject report = new JsonObject();
                report.addProperty("action","SERVER_FAIL");
                report.addProperty("content",brokerAddr);

                try{
                    Socket socket = new Socket(zookeeper_ip,zookeeper_port);
                    PrintStream writer = new PrintStream(socket.getOutputStream());
                    writer.println(report.toString());
                }catch(Exception e1){
                    System.out.println("cannot connect to zookeeper");
                }

                try {
                    Thread.sleep(5000);
                } catch (Exception se) {
                    System.out.println(se.getMessage());
                }
                brokerAddr = getServerAddr(topic);
                topicBrokerMap.replace(topic, brokerAddr);
            } finally {
//                if (client != null) {
//                    try {
//                        client.close();
//                    } catch (Exception e) {
//                        System.out.println(e.getMessage());
//                    }
//                }
            }
        }

    }

    public void run() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Please input topics: ");
        String input = scanner.nextLine();
        String[] topics = input.split(",");
        topicList = Arrays.asList(topics);

        while (true) {
            try {
                sleep(5000);
            } catch (Exception e) {
                e.printStackTrace();
            }
            //create message
            JsonObject msg = CreateMsg();

            //parse msg topic
            JsonObject content = msg.get("content").getAsJsonObject();
            String topic = content.get("topic").getAsString();
            System.out.println(topic);

            String brokerAddr = null;
            if (topicBrokerMap.containsKey(topic))
                brokerAddr = topicBrokerMap.get(topic);
            else {
                brokerAddr = getServerAddr(topic);
                if (brokerAddr == null) continue;
                topicBrokerMap.put(topic, brokerAddr);
                System.out.println(brokerAddr);
            }

            //send msg
            sendMsg(brokerAddr, topic, msg);


        }
    }

    public static void main(String[] args) {
        Publisher pub = new Publisher();
        pub.run();
    }
}