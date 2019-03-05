package zookeeper;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Zookeeper {

    public Zookeeper() {

    }

    //private Map<String, Set<String>> ServerClientMap = new HashMap<>();
    private Map<String, Set<String>> TopicToSever = new HashMap<>();
    private Set<String> serverSet = new HashSet<>();
    private Map<String, String> leaderMap = new HashMap<>();
    private Map<String, Set<String>> serverTopicSet = new HashMap<>();
    private int zookeeper_port = 8889;
    private int defaultGroupSize = 5;
    private BrokerLoad loads = new BrokerLoad();

    private void run() {
        ServerSocket listener = null;
        try {
            listener = new ServerSocket(zookeeper_port);
            System.out.println("Server listening at port " + zookeeper_port);
        } catch (Exception e) {
            System.out.println("Exception starting zookeeper.");
            System.out.println(e.getMessage());
            return;
        }

        Routine routine = new Routine(20000);
        routine.start();

        ExecutorService threadPool = Executors.newCachedThreadPool();

        while (true) {
            try {
                Socket conn = listener.accept();

                TaskHandler task = new TaskHandler(conn);
                threadPool.execute(task);

            } catch (Exception e) {
                System.out.println("Exception handling connection");
                System.out.println(e.getMessage());
            }
        }
    }

    class Routine extends Thread {
        long t;

        public Routine(long time) {
            t = time;
        }

        public void run() {
            while (true) {
                boolean reallocate = false;
                if (!serverSet.isEmpty()) {
                    Set<String> brokers = new HashSet<>(serverSet);
                    for(String broker:brokers){
                        if(!checkAlive(broker)){
                            delServer(broker);
                            reallocate = true;
                        }
                    }

                    if(reallocate)
                        allocateServer(null);
                }

                try {
                    Thread.sleep(t);
                } catch (Exception e) {
                    System.out.println("routine sleep fail");
                    System.out.println(e.getMessage());
                }
            }
        }
    }

    private void allocateServer(String brokerAddr) {

        if(brokerAddr==null){

        }else{
            Set<String> topics = getTopicSet();
            for(String topic:topics){
                int size = TopicToSever.get(topic).size();
                if(size!=0&&size<defaultGroupSize&&serverSet.size()>defaultGroupSize){
                    int num = defaultGroupSize-size;
                    Set<String> brokerGroup = loads.chooseKey(num+1);
                    Iterator<String> it = brokerGroup.iterator();
                    String leader = leaderMap.get(topic);
                    while(num>0){
                        //get a broker with min load and assign it to the topic
                        String broker = it.next();
                        if(!leader.equals(broker)){
                            //what is the broker is already in the topic?
                            loads.inc(broker);
                            num--;
                        }
                    }
                }
            }
        }
    }

    private void getServerCount() {

    }

    private Set<String> getTopicSet() {
        return TopicToSever.keySet();
    }

    private void formGroup(String topic) {
        //determine group size
        int size = serverSet.size();
        if (size > defaultGroupSize) {
            size = defaultGroupSize;
        }

        //select brokers
        Set<String> brokerGroup = loads.chooseKey(size);
        TopicToSever.put(topic, brokerGroup);
        Iterator<String> it = brokerGroup.iterator();
        while (it.hasNext()) {
            String broker = it.next();
            loads.inc(broker);
            if (serverTopicSet.containsKey(broker))
                serverTopicSet.get(broker).add(topic);
            else {
                Set<String> topicSet = new HashSet<>();
                topicSet.add(topic);
                serverTopicSet.put(broker, topicSet);
            }
        }

    }

    private void leaderElection(String topic) {
        //random choose a leader in the broker group
        Set<String> brokerSet = TopicToSever.get(topic);
        String leader = null;
        if (!brokerSet.isEmpty()) {
            String[] brokers = brokerSet.toArray(new String[brokerSet.size()]);
            int i = (int) (Math.random() * brokerSet.size());
            leader = brokers[i];
        }
        if (leaderMap.containsKey(topic)) leaderMap.replace(topic, leader);
        else leaderMap.put(topic, leader);
    }

    private void noticeTopicLeader(String topic) {
        //tell the topic leader to build spanning tree for routing message
        String leaderAddr = leaderMap.get(topic);

        if (leaderAddr != null) {
            int i = leaderAddr.indexOf(":");
            String ip = leaderAddr.substring(0, i);
            int port = Integer.parseInt(leaderAddr.substring(i + 1));

            JSONObject msg = new JSONObject();
            msg.put("action", "BUILD_SPANNING_TREE");
            JSONObject content = new JSONObject();
            content.put("topic", topic);

            Set<String> brokers = TopicToSever.get(topic);
            StringBuilder brokerList = new StringBuilder();
            for (String broker : brokers) {
                brokerList.append(broker + ",");
            }
            brokerList.deleteCharAt(brokerList.length() - 1);

            content.put("brokers", brokerList.toString());
            msg.put("content", content);

            try {
                Socket client = new Socket(ip, port);
                PrintStream writer = new PrintStream(client.getOutputStream());
                writer.println(msg.toString());

                writer.close();
                client.close();
            } catch (Exception e) {
                System.out.println("error in notice leader");
                System.out.println(e.getMessage());
            }
        }
    }

    private String getLeaderInfo(String topic) {
        //return leader info for a specific topic

        if (!getTopicSet().contains(topic)) {
            formGroup(topic);
            leaderElection(topic);
            noticeTopicLeader(topic);
        }
        return leaderMap.get(topic);
    }

    private boolean checkAlive(String brokerAddr) {
        //check if a broker is online
        //brokerAddr format IP:port
        int i = brokerAddr.indexOf(":");
        String ip = brokerAddr.substring(0, i);
        int port = Integer.parseInt(brokerAddr.substring(i + 1));

        try {
            Socket client = new Socket(ip, port);

            JSONObject msg = new JSONObject();
            msg.put("action", "CHECK_ALIVE");

            PrintStream writer = new PrintStream(client.getOutputStream());
            writer.println(msg.toString());

            writer.close();
            client.close();
            return true;
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return false;
        }
    }

    private void pingServer() {

    }

    private void addServer(String topic, String brokerAddr) {
        serverSet.add(brokerAddr);
        loads.inc(brokerAddr);
        if (topic == null) {
            allocateServer(brokerAddr);
        } else {
            loads.inc(brokerAddr);

            TopicToSever.get(topic).add(brokerAddr);
            if (serverTopicSet.containsKey(brokerAddr))
                serverTopicSet.get(brokerAddr).add(topic);
            else {
                Set<String> topics = new HashSet<>();
                topics.add(topic);
                serverTopicSet.put(brokerAddr, topics);
            }

            if (leaderMap.get(topic) == null) {
                leaderMap.replace(topic, brokerAddr);
                noticeTopicLeader(topic);
            } else {
                String leaderAddr = leaderMap.get(topic);
                JSONObject msg = new JSONObject();
                msg.put("action", "ADD_BROKER");
                JSONObject content = new JSONObject();
                content.put("topic", topic);
                content.put("broker", brokerAddr);
                msg.put("content", content);

                int i = leaderAddr.indexOf(":");
                String ip = leaderAddr.substring(0, i);
                int port = Integer.parseInt(leaderAddr.substring(i + 1));
                try {
                    Socket socket = new Socket(ip, port);
                    PrintStream writer = new PrintStream(socket.getOutputStream());
                    writer.println(msg.toString());
                } catch (Exception e) {
                    System.out.println("error in adding broker");
                    System.out.println(e.getMessage());
                }
            }
        }
    }

    private void delServer(String brokerAddr) {
        //find all topics managed by broker
        Set<String> topics = serverTopicSet.get(brokerAddr);
        loads.removeKey(brokerAddr);

        for (String topic : topics) {
            TopicToSever.get(topic).remove(brokerAddr);
            if (leaderMap.get(topic).equals(brokerAddr)) {
                leaderElection(topic);
            }
            String leaderAddr = leaderMap.get(topic);
            if (leaderAddr != null) {
                JSONObject msg = new JSONObject();
                msg.put("action", "BROKER_FAIL");
                JSONObject content = new JSONObject();
                content.put("topic", topic);
                content.put("broker", brokerAddr);
                msg.put("content", content);

                int i = leaderAddr.indexOf(":");
                String ip = leaderAddr.substring(0, i);
                int port = Integer.parseInt(leaderAddr.substring(i + 1));
                try {
                    Socket socket = new Socket(ip, port);
                    PrintStream writer = new PrintStream(socket.getOutputStream());
                    writer.println(msg.toString());
                } catch (Exception e) {
                    System.out.println("error in reporting broker failure");
                    System.out.println(e.getMessage());
                }
            }
        }

        serverSet.remove(brokerAddr);
        serverTopicSet.remove(brokerAddr);
    }

    private boolean addClient(String topic, String clientAddr) {
        String leaderAddr = leaderMap.get(topic);

        if (leaderAddr != null) {
            JSONObject msg = new JSONObject();
            msg.put("action", "ALLOCATE_CLIENT");
            JSONObject content = new JSONObject();
            content.put("topic", topic);
            content.put("client", clientAddr);
            msg.put("content", content);

            int i = leaderAddr.indexOf(":");
            String ip = leaderAddr.substring(0, i);
            int port = Integer.parseInt(leaderAddr.substring(i + 1));

            try {
                Socket client = new Socket(ip, port);
                PrintStream writer = new PrintStream(client.getOutputStream());
                writer.println(msg.toString());
                return true;
            } catch (Exception e) {
                System.out.println("error in adding client");
                System.out.println(e.getMessage());
                return false;
            }
        } else {
            return false;
        }
    }

    private void delClient(String topic, String clientAddr) {

    }

    class TaskHandler extends Thread {
        Socket conn;
        BufferedReader reader;
        PrintStream writer;

        public TaskHandler(Socket conn) {
            this.conn = conn;
            try {
                reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                writer = new PrintStream(conn.getOutputStream());
            } catch (IOException e) {
                System.out.println("cannot read/write in thread.");
                System.out.println(e.getMessage());
            }
        }

        public void run() {

            try {
                JSONObject sent = new JSONObject();

                String inputLine = reader.readLine();
                JSONParser parser = new JSONParser();
                JSONObject json = (JSONObject) parser.parse(inputLine);
                String action = (String) json.get("action");

                //for "NEW_TOPIC", return topic leader info to publisher
                if (action.equals("NEW_TOPIC")) {
                    //content is topic
                    String topic = (String) json.get("content");

                    String leaderAddr = getLeaderInfo(topic);

                    sent.put("action", "NEW_TOPIC");
                    sent.put("content", leaderAddr);
                    writer.println(sent.toString());
                } else if (action.equals("CLIENT_REGISTER")) {
                    JSONObject content = (JSONObject) json.get("content");
                    String sender = (String) content.get("sender");
                    String topic = (String) content.get("topic");

                    sent.put("action", "CLIENT_REGISTER");
                    if (addClient(topic, sender)) {
                        sent.put("content", "success");
                    } else {
                        sent.put("content", "fail");
                    }
                    writer.println(sent.toString());
                }


                reader.close();
                writer.close();
                conn.close();
            } catch (Exception e) {
                System.out.println("Exception handling request.");
                System.out.println(e.getMessage());
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                }
            }

        }
    }

    public static void main(String[] args) {
        Zookeeper zookeeper = new Zookeeper();
        zookeeper.run();
    }
}

