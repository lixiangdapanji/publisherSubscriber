package zookeeper;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class Zookeeper {

    public Zookeeper() {

    }

    //private Map<String, Set<String>> ServerClientMap = new HashMap<>();
    private Map<String, Set<String>> TopicToSever = new HashMap<>();
    private Set<String> server = new HashSet<>();
    private Map<String, String> leaderMap = new HashMap<>();
    private Map<String, Set<String>> serverTopicSet = new HashMap<>();
    private int zookeeper_port = 8889;
    private int groupSize = 5;

    private void run() {
        int count = 0;
        try {
            ServerSocket listener = new ServerSocket(zookeeper_port);
            System.out.println("Server listening at port " + zookeeper_port);

            while (true) {
                Socket conn = listener.accept();
                BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                Writer writer = new OutputStreamWriter(conn.getOutputStream());
                JSONObject sent = new JSONObject();

                StringBuilder sb = new StringBuilder();
                String inputLine;
                while((inputLine = reader.readLine())!=null){
                    sb.append(inputLine);
                }
                JSONParser parser = new JSONParser();
                JSONObject json = (JSONObject) parser.parse(sb.toString());
                String action = (String) json.get("action");
                if (action.equals("NEW_TOPIC")) {
                    if (count == 0) {
                        sent.put("action", "NEW_TOPIC");
                        sent.put("content", "127.0.0.1:8080");
                        count++;
                        writer.write(sent.toString());
                        writer.flush();
                    } else {
                        sent.put("action", "NEW_TOPIC");
                        sent.put("content", "127.0.0.1:8889");
                        writer.write(sent.toString());
                        writer.flush();
                    }
                }
                if (action.equals("NEW_FEED")) {
                    JSONObject content = (JSONObject) json.get("content");
                    String topic = (String) content.get("topic");
                    String msg = (String) content.get("msg");
                    System.out.println(topic + ": " + msg);
                }

                reader.close();
                writer.close();
                conn.close();
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private void allocatePublisher() {

    }

    private void getServerCount() {

    }

    private Set<String> getTopicSet(){
        return TopicToSever.keySet();
    }

    private void addTopic(String topic){

    }

    private void formGroup(String topic){

    }

    private void leaderElection(String topic) {
        //
        while (true) {




            if (noticeTopicLeader(topic)) return;
        }
    }

    private boolean noticeTopicLeader(String topic) {
        //tell the topic leader to build spanning tree for routing message
        //if leader is online return true, else return false
        String leaderAddr = leaderMap.get(topic);
        int i = leaderAddr.indexOf(":");
        String ip = leaderAddr.substring(0, i);
        int port = Integer.parseInt(leaderAddr.substring(i + 1));

        JSONObject msg = new JSONObject();
        msg.put("action", "BUILD_SPANNING_TREE");
        JSONObject content = new JSONObject();
        content.put("topic",topic);

        Set<String> brokers = TopicToSever.get(topic);
        StringBuilder brokerList = new StringBuilder();
        for(String broker: brokers){
            brokerList.append(broker + ",");
        }
        brokerList.deleteCharAt(brokerList.length()-1);

        content.put("brokers",brokerList.toString());
        msg.put("content",content);

        try{
            Socket client = new Socket(ip,port);
            Writer writer = new OutputStreamWriter(client.getOutputStream());
            writer.write(msg.toString());
            writer.flush();

            writer.close();
            client.close();
            return true;
        }catch(Exception e){
            System.out.println(e.getMessage());
            return false;
        }
    }

    private String getLeaderInfo(String topic) {
        //return leader info for a specific topic
        if(!getTopicSet().contains(topic)){
            addTopic(topic);
        }
        String leaderAddr = leaderMap.get(topic);

        //if leader is offline, run leader election and get the new leader info
        while(!checkAlive(leaderAddr)){
            leaderElection(topic);
            leaderAddr = leaderMap.get(topic);
        }

        return leaderAddr;
    }

    private boolean checkAlive(String brokerAddr) {
        //check if a broker is online
        //brokerAddr format IP:port
        int i = brokerAddr.indexOf(":");
        String ip = brokerAddr.substring(0, i);
        int port = Integer.parseInt(brokerAddr.substring(i + 1));

        try{
            Socket client = new Socket(ip,port);

            JSONObject msg = new JSONObject();
            msg.put("action", "CHECK_ALIVE");

            Writer writer = new OutputStreamWriter(client.getOutputStream());
            writer.write(msg.toString());
            writer.flush();

            writer.close();
            client.close();
            return true;
        }catch(Exception e){
            System.out.println(e.getMessage());
            return false;
        }
    }

    private void pingServer() {

    }

    private void addServer() {

    }

    private void delServer() {

    }

    private void addClient() {

    }

    private void delClient() {

    }

    public static void main(String[] args) {
        Zookeeper zookeeper = new Zookeeper();
        zookeeper.run();
    }
}

