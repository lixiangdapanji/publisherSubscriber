package zookeeper;

import message.MessageAction;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import util.JsonUtil;

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
            e.printStackTrace();
            return;
        }

        Routine routine = new Routine(30000);
        routine.start();

        ExecutorService threadPool = Executors.newFixedThreadPool(10);

        while (true) {
            try {
                Socket conn = listener.accept();

                TaskHandler task = new TaskHandler(conn);
                threadPool.execute(task);

            } catch (Exception e) {
                System.out.println("Exception handling connection");
                e.getStackTrace();
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
                    for (String broker : brokers) {
                        if (!checkAlive(broker)) {
                            delServer(broker);
                            //reallocate = true;
                        }
                    }

                    if (reallocate)
                        allocateServer("");
                }

                try {
                    Thread.sleep(t);
                } catch (Exception e) {
                    System.out.println("routine sleep fail");
                    e.getStackTrace();
                }
            }
        }
    }

    private void allocateServer(String brokerAddr) {

        if (brokerAddr.equals("")) {
            if (serverSet.size() < defaultGroupSize) return;
            Set<String> topics = getTopicSet();
            for (String topic : topics) {
                int size = TopicToSever.get(topic).size();
                if (size != 0 && size < defaultGroupSize) {

                    String leaderAddr = leaderMap.get(topic);
                    int i = leaderAddr.indexOf(":");
                    String ip = leaderAddr.substring(0, i);
                    int port = Integer.parseInt(leaderAddr.substring(i + 1));

                    JSONObject msg = new JSONObject();
                    msg.put("action", "ADD_NEW_BROKER");
                    JSONObject content = new JSONObject();
                    content.put("topic", topic);

                    //get an array of brokers with load ascending
                    List<String> brokerGroup = loads.chooseKey(defaultGroupSize);
                    int idx = 0;
                    int num = defaultGroupSize - size;
                    while (num > 0) {
                        String broker = brokerGroup.get(idx);
                        //if broker not in topic server set, add it
                        if (!TopicToSever.get(topic).contains(broker)) {
                            if (content.containsKey("broker")) content.replace("broker", broker);
                            else content.put("broker", broker);

                            if (msg.containsKey("content")) msg.replace("content", content);
                            else msg.put("content", content);

                            try {
                                Socket client = new Socket(ip, port);
                                PrintStream writer = new PrintStream(client.getOutputStream());
                                writer.println(msg.toString());

                                loads.inc(broker);
                                TopicToSever.get(topic).add(broker);
                                serverTopicSet.get(broker).add(topic);
                                num--;
                            } catch (Exception e) {
                                System.out.println("error in load balancing");
                                System.out.println(e.getMessage());
                            }
                        }
                        idx++;
                    }
                }
            }
        } else {
            //find a topic with least number of brokers
            String topic = null;
            int num = Integer.MAX_VALUE;
            Set<String> topics = getTopicSet();
            for (String t : topics) {
                int size = TopicToSever.get(topic).size();
                if (size != 0 && size < num) {
                    num = size;
                    topic = t;
                }
            }

            //add broker to the topic
            if (num < defaultGroupSize) {
                String leaderAddr = leaderMap.get(topic);

                int i = leaderAddr.indexOf(":");
                String ip = leaderAddr.substring(0, i);
                int port = Integer.parseInt(leaderAddr.substring(i + 1));

                JSONObject msg = new JSONObject();
                msg.put("action", "ADD_BROKER");
                JSONObject content = new JSONObject();
                content.put("topic", topic);
                content.put("broker", brokerAddr);
                msg.put("content", content);

                try {
                    Socket client = new Socket(ip, port);
                    PrintStream writer = new PrintStream(client.getOutputStream());
                    writer.println(msg.toString());
                    loads.inc(brokerAddr);
                    TopicToSever.get(topic).add(brokerAddr);
                    serverTopicSet.put(brokerAddr, new HashSet<>());
                    serverTopicSet.get(brokerAddr).add(topic);
                } catch (Exception e) {
                    System.out.println("error in add broker to topic");
                    System.out.println(e.getMessage());
                }
            }
        }
    }

    //private void getServerCount() {

    //}

    private Set<String> getTopicSet() {
        return TopicToSever.keySet();
    }

    private void formGroup(String topic) {

        //determine group size
        int size = serverSet.size();
        System.out.println("Forming group from " + size + " brokers");
        if (size == 0) {
            return;
        }
        if (size > defaultGroupSize) {
            size = defaultGroupSize;
        }

        //select brokers
        List<String> brokerGroup = loads.chooseKey(size);
        TopicToSever.put(topic, new HashSet<>());
        Iterator<String> it = brokerGroup.iterator();
        String broker = it.next();
        leaderMap.put(topic, broker);
        loads.inc(broker);

        while (true) {
            loads.inc(broker);
            TopicToSever.get(topic).add(broker);
            if (!serverTopicSet.containsKey(broker))
                serverTopicSet.put(broker, new HashSet<>());
            serverTopicSet.get(broker).add(topic);

            if (it.hasNext()) broker = it.next();
            else break;
        }

    }

    private void leaderElection(String topic) {
        //choose a leader in the broker group with minimum load
        Set<String> brokerSet = TopicToSever.get(topic);
        String leader = null;
        if (!brokerSet.isEmpty()) {
            String[] brokers = brokerSet.toArray(new String[brokerSet.size()]);
            leader = loads.chooseMin(brokers);
        }

        leaderMap.put(topic, leader);
        if (leader != null)
            loads.inc(leader);
    }

    private void noticeTopicLeader(String topic) {
        //tell the topic leader to build spanning tree for routing message
        String leaderAddr = leaderMap.get(topic);
        System.out.println("notice leader: " + leaderAddr);
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

            content.put("brokers", brokerList.toString());
            msg.put("content", content);

            try {
                Socket client = new Socket(ip, port);
                PrintStream writer = new PrintStream(client.getOutputStream());
                writer.println(msg.toString());
                System.out.println(ip + ":" + port);
            } catch (Exception e) {
                System.out.println("error in notice leader");
                e.getStackTrace();
            }
        }
    }

    private String getLeaderInfo(String topic) {
        //return leader info for a specific topic
        //if it's a new topic, form a brokers group first
        if (!getTopicSet().contains(topic)) {
            formGroup(topic);
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
            System.out.println("broker fail");
            e.getStackTrace();
        }
        return false;
    }

    //private void pingServer() {

    //}

    private void addServer(String topicSet, String brokerAddr) {
        System.out.println("Adding server: " + brokerAddr);
        //if it's a new broker, add it to broker set
        if (!serverSet.contains(brokerAddr)) {
            serverSet.add(brokerAddr);
            loads.inc(brokerAddr);
        }

        String[] topics = topicSet.split(",");
        for (String topic : topics) {
            if (!topic.equals("")) {
                loads.inc(brokerAddr);

                boolean newTopic = false;
                if (!TopicToSever.containsKey(topic)) {
                    TopicToSever.put(topic, new HashSet<>());
                    newTopic = true;
                }
                TopicToSever.get(topic).add(brokerAddr);

                if (!serverTopicSet.containsKey(brokerAddr))
                    serverTopicSet.put(brokerAddr, new HashSet<>());
                serverTopicSet.get(brokerAddr).add(topic);

                if (newTopic || leaderMap.get(topic) == null) {
                    leaderMap.put(topic, brokerAddr);
                    noticeTopicLeader(topic);
                    loads.inc(brokerAddr);
                } else {
                    String leaderAddr = leaderMap.get(topic);
                    JSONObject msg = new JSONObject();
                    msg.put("action", "ADD_NEW_BROKER");
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
                        e.getStackTrace();
                    }
                }
            }
        }
    }

    private void delServer(String brokerAddr) {
        if (brokerAddr == null || !serverSet.contains(brokerAddr)) return;
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
                msg.put("action", "SERVER_FAIL");
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
                    e.getStackTrace();
                }
            }
        }

        serverSet.remove(brokerAddr);
        serverTopicSet.remove(brokerAddr);
    }

    private boolean addClient(String topic, String clientAddr) {
        System.out.println("add client on " + topic + ", client address: " + clientAddr);
        String leaderAddr = leaderMap.get(topic);

        if (leaderAddr != null) {
            JSONObject msg = new JSONObject();
            msg.put("action", MessageAction.ALLOCATE_CLIENT);
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

    private boolean delClient(String topic, String clientAddr) {
        String leaderAddr = leaderMap.get(topic);

        if (leaderAddr != null) {
            JSONObject msg = new JSONObject();
            msg.put("action", "DEL_CLIENT");
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
                System.out.println("error in deleting client");
                System.out.println(e.getMessage());
                return false;
            }
        } else {
            return false;
        }
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

                System.out.println("Action: " + action);
                //for "NEW_TOPIC", return topic leader info to publisher
                if (action.equals("NEW_TOPIC")) {
                    //content is topic
                    String topic = (String) json.get("content");

                    String leaderAddr = getLeaderInfo(topic);
                    System.out.println("leader address : " + leaderAddr);
                    sent.put("action", "NEW_TOPIC");
                    sent.put("content", leaderAddr);
                    writer.println(sent.toString());
                } else if (action.equals("CLIENT_REGISTER")) {
                    JSONObject content = (JSONObject) json.get("content");
                    String sender = (String) json.get("sender");
                    String topic = (String) content.get("topic");

                    sent.put("action", "CLIENT_REGISTER");
                    if (addClient(topic, sender)) {
                        sent.put("content", "success");
                    } else {
                        sent.put("content", "fail");
                    }
                    writer.println(sent.toString());
                } else if (action.equals("BROKER_REG")) {
                    String brokerAddr = (String) json.get("sender");
                    String content = (String) json.get("content");
                    addServer(content, brokerAddr);
                } else if (action.equals("GET_TOPIC")) {
                    Set<String> topicSet = getTopicSet();
                    StringBuilder topics = new StringBuilder();
                    for (String topic : topicSet) {
                        topics.append(topic + ",");
                    }
                    sent.put("action", "TOPICS");
                    sent.put("content", topics.toString());
                    writer.println(sent.toString());
                } else if (action.equals("SERVER_FAIL")) {
                    String content = (String) json.get("content");
                    String[] brokers = content.split(",");
                    for (String broker : brokers)
                        delServer(broker);
                } else {
                    System.out.println("action not supported");
                }


                reader.close();
                writer.close();
                conn.close();
            } catch (Exception e) {
                System.out.println("Exception handling request.");
                e.printStackTrace();
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