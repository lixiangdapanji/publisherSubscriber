package publisher;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

public class Publisher {
    private final String zookeeper_ip = "127.0.0.1";
    private int zookeeper_port = 8889;
    private Map<String, String> topicBrokerMap = new HashMap<>();
    private List<String> topicList = new ArrayList<>();

    public Publisher() {
        //initial topic list
        topicList.add("Topic_1");
        topicList.add("Topic_2");
        topicList.add("Topic_3");
        topicList.add("Topic_4");
        topicList.add("Topic_5");
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
        Writer writer = null;
        BufferedReader reader = null;
        String brokerAddr = null;

        JSONObject sent = new JSONObject();
        sent.put("action", "NEW_TOPIC");
        sent.put("content", topic);

        try {
            client = new Socket(zookeeper_ip, zookeeper_port);
            writer = new OutputStreamWriter(client.getOutputStream());
            writer.write(sent.toString());
            writer.flush();

            /*
            received message { sender: zookeeper,
                               topic: topic,
                               content: broker addr }
             */
            reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
            StringBuilder sb = new StringBuilder();
            String inputLine;
            while((inputLine = reader.readLine())!=null){
                sb.append(inputLine);
            }
            JSONParser parser = new JSONParser();
            JSONObject json = (JSONObject) parser.parse(sb.toString());
            brokerAddr = (String) json.get("content");

            writer.close();
            reader.close();
            client.close();
        } catch (UnknownHostException e) {
            System.out.println("Zookeeper not found!");
            System.out.println(e.getMessage());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            try {
                if (client != null)
                    client.close();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }

        return brokerAddr;
    }

    private void sendMsg(String brokerAddr, String topic, JSONObject msg) {
        //parse brokerAddr ip:port
        int i = brokerAddr.indexOf(":");
        String ip = brokerAddr.substring(0, i);
        int port = Integer.parseInt(brokerAddr.substring(i + 1));

        Socket client = null;
        Writer writer = null;
        while (true) {
            try {
                client = new Socket(ip, port);
                writer = new OutputStreamWriter(client.getOutputStream());
                writer.write(msg.toString());
                writer.flush();

                writer.close();
                client.close();
                break;
            } catch (Exception e) {
                System.out.println(e.getMessage());
                brokerAddr = getServerAddr(topic);
                topicBrokerMap.replace(topic, brokerAddr);
            } finally {
                try {
                    if (client != null)
                        client.close();
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        }

    }

    private void run() {
        while (true) {
            //create message
            JSONObject msg = CreateMsg();

            //parse msg topic
            JSONObject content = (JSONObject) msg.get("content");
            String topic = (String) content.get("topic");
            String brokerAddr = null;
            if (topicBrokerMap.containsKey(topic))
                brokerAddr = topicBrokerMap.get(topic);
            else {
                brokerAddr = getServerAddr(topic);
                topicBrokerMap.put(topic, brokerAddr);
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
