package publisher;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

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

    private JSONObject CreateMsg() {
        /*
        message format { sender: publisher,
                         action: NEW_FEED
                         content: { topic: topic,
                                    msg: content }
                        }
         */
        int size = topicList.size();

        JSONObject msg = new JSONObject();
        msg.put("action", "NEW_FEED");

        JSONObject content = new JSONObject();
        content.put("topic", topicList.get((int) (Math.random() * size)));
        content.put("msg", "a new message");

        msg.put("content", content);

        return msg;
    }

    private String getServerAddr(String topic) {
        Socket client = null;
        BufferedReader reader = null;
        String brokerAddr = null;
        PrintStream writer = null;

        JSONObject sent = new JSONObject();
        sent.put("action", "NEW_TOPIC");
        sent.put("content", topic);

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

            JSONParser parser = new JSONParser();
            JSONObject json = (JSONObject) parser.parse(inputLine);
            brokerAddr = (String) json.get("content");

            writer.close();
            reader.close();
            client.close();
        } catch (Exception e) {
            System.out.println("Exception in getServerAddr.");
            e.printStackTrace();
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

    private void sendMsg(String brokerAddr, String topic, JSONObject msg) {
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
                JSONObject report = new JSONObject();
                report.put("action","SERVER_FAIL");
                report.put("content",brokerAddr);

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
            JSONObject msg = CreateMsg();

            //parse msg topic
            JSONObject content = (JSONObject) msg.get("content");
            String topic = (String) content.get("topic");
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