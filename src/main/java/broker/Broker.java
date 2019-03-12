package broker;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import message.MessageAction;

import util.JsonUtil;

import java.io.*;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Broker {

    private String ip = "127.0.0.1";
    private int port;
    private JsonParser parser;
    private String id;
    //private AllOne allOne;
    private String zookeeperIp;
    private int zookeeperPort;

    //server -> client list map; Key: server ip addr:port:topic; value: client list(client: ip:port)
    private Map<String, Set<String>> serverClientMap;

    private Map<String, Set<String>> topicServerMap;

    //
    private Map<String, Set<String>> routingMap;

    private Map<String, Integer> serverLoad;

    private Map<String, String> leaderMap;

    //给message编号
    private Map<String, AtomicInteger> topicCount;
    //private AtomicInteger msgCount;

    private Map<String, List<String>> topicMsgMap;

    public Broker() {
    }

    public Broker(String ip, int port) {
        setIPAndPort(ip, port);
        init();
    }

    public void init() {
        serverClientMap = new HashMap<>();
        topicServerMap = new HashMap<>();
        routingMap = new HashMap<>();
        //msgCount = new AtomicInteger(0);
        parser = new JsonParser();
        //allOne = new AllOne();
        serverLoad = new HashMap<>();
        topicMsgMap = new HashMap<>();
        leaderMap = new HashMap<>();
        topicCount = new HashMap<>();
        topicMsgMap = new HashMap<>();
    }

    public void setIPAndPort(String ip, int port) {
        this.ip = ip;
        this.port = port;
        id = ip + ":" + port;
    }

    public void setZookeeper(String ip, int port) {
        this.zookeeperIp = ip;
        this.zookeeperPort = port;
    }

    /**
     * leader receive new feed. Add count to this message and routing server to send message to all clients.
     *
     * @param message{sender: zookeeper,
     *                        action: NEW_FEED,
     *                        content:{topic:topic,
     *                        message: message}}
     */
    private void newFeed(JsonObject message) {
        System.out.println("newFeed is called.");
        JsonObject content = message.get("content").getAsJsonObject();
        String topic = content.get("topic").getAsString();
        String messagecontent = content.get("msg").getAsString();

        int num = topicCount.get(topic).incrementAndGet();
        content.addProperty("id", num + "");
        //content.put("message", messagecontent);
        JsonObject object = new JsonObject();
        object.addProperty("sender", id);
        object.addProperty("action", MessageAction.SEND_MESSAGE);
        object.add("content", content);

        sendMsg(object);
        //routingServer(topic, object);
    }


    /**
     * add client
     *
     * @param message {sender: broker,
     *                action: ADD_CLIENT,
     *                content:{topic: topic
     *                server: ip:port,
     *                client: ip:port}}
     */
    private void addClient(JsonObject message) {
        System.out.println("addclient is called.");
        try {
            JsonObject content = (JsonObject) message.get("content");
            String serverId = content.get("server").getAsString();
            String clientId = content.get("client").getAsString();
            String topic = content.get("topic").getAsString();
            String key = serverId + ":" + topic;
            if (!serverClientMap.containsKey(key)) {
                serverClientMap.put(key, new HashSet<>());
            }
            serverClientMap.get(key).add(clientId);
            serverLoad.put(serverId, serverLoad.getOrDefault(serverId, 0) + 1);
            //message.put("sender")
            routingServer(topic, message);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * delete client
     *
     * @param message{sender: broker,
     *                        action: DEL_CLIENT,
     *                        content:{topic: topic,
     *                        client: ip:port}}
     * @return count of changes. -1:exception.
     */
    private int delClient(JsonObject message) {
        System.out.println("delClient is called.");
        try {
            //int count = 0;
            JsonObject content =  message.get("content").getAsJsonObject();
            String topic = content.get("topic").getAsString();
            String clientId = content.get("client").getAsString();
            Set<String> serverSet = topicServerMap.get(topic);
            for (String serverId : serverSet) {
                String key = serverId + ":" + topic;
                if (serverClientMap.containsKey("key") && serverClientMap.get(key).contains(clientId)) {
                    serverClientMap.get(key).remove(clientId);
                    serverLoad.put(serverId, serverLoad.get(serverId) - 1);
                    return 1;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    /**
     * load balancing choosing a server to control this client.
     *
     * @param message {sender: zookeeper,
     *                action: ALLOCATE_CLIENT,
     *                content:{topic: topic 1,
     *                client: clientId}}
     */
    private void allocateClient(JsonObject message) {
        System.out.println("allocateClient is called.");
        try {
            JsonObject content = message.get("content").getAsJsonObject();
            String topic = content.get("topic").getAsString();
            String clientId = content.get("client").getAsString();
            String minLoad = "";
            int min = Integer.MAX_VALUE;
            System.out.println("allocate clients in " + topicServerMap.get(topic).size() + " servers");
            for (String serverId : topicServerMap.get(topic)) {
                if (!serverLoad.containsKey(serverId)) {
                    minLoad = serverId;
                    break;
                } else {
                    if (serverLoad.get(serverId) < min) {
                        min = serverLoad.get(serverId);
                        minLoad = serverId;
                    }
                }
            }


            JsonObject addClient = new JsonObject();
            addClient.addProperty("sender", id);
            addClient.addProperty("action", "ADD_CLIENT");
            JsonObject sendContent = new JsonObject();
            sendContent.addProperty("topic", topic);
            sendContent.addProperty("server", minLoad);
            sendContent.addProperty("client", clientId);
            addClient.add("content", sendContent);
            System.out.println("Add client : " + addClient.toString());
            addClient(addClient);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * build spanning tree
     *
     * @param message{sender: zookeeper,
     *                        action: BUILD_SPANNING_TREE,
     *                        content:{topic: topic,
     *                        brokers: "server1, server2"}
     */
    private void buildSpanningTree(JsonObject message) {
        System.out.println("buildSpanningTree is called.");
        JsonObject content = message.get("content").getAsJsonObject();
        String topic = content.get("topic").getAsString();
        String failed = !content.has("failed") ? "" : content.get("failed").getAsString();

        leaderMap.put(topic, id);
        topicServerMap.put(topic, new HashSet<>());

        if (!topicCount.containsKey(topic)) {
            topicCount.put(topic, new AtomicInteger(0));
        }

        String brokers = content.get("brokers").getAsString();
        String[] brokerlist = brokers.split(",");

        for (String broker : brokerlist) {
            topicServerMap.get(topic).add(broker);
        }


        List<Integer> red = new ArrayList<>();
        List<Integer> blue = new ArrayList<>();

        Random random = new Random();
        int n = brokerlist.length;
        //System.out.println("buildSpanningTree " + n + " brokers");
        for (int i = 0; i < n; i++) {
            blue.add(i);
        }

        int[][] graph = new int[n][n];
        int start = random.nextInt(n);
        red.add(start);
        blue.remove(start);

        while (blue.size() > 0) {
            //int b = random.nextInt(blue.size());
            int r = random.nextInt(red.size());
            graph[red.get(r)][blue.get(blue.size() - 1)] = 1;
            graph[blue.get(blue.size() - 1)][red.get(r)] = 1;
            blue.remove(blue.size() - 1);
        }

        Map<String, String> routing = new HashMap<>();
        for (int i = 0; i < n; i++) {
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < n; j++) {
                if (graph[i][j] == 1) {
                    sb.append(brokerlist[j]);
                    sb.append(",");
                }
            }
            routing.put(brokerlist[i], sb.toString());
        }

        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                System.out.print(graph[i][j] + " ");
            }
            System.out.println();
        }


        for (Map.Entry<String, String> entry : routing.entrySet()) {
            String key = entry.getKey();
            JsonObject object = new JsonObject();
            object.addProperty("sender", id);
            object.addProperty("action", MessageAction.REBUILD_EDGES);
            JsonObject sendContent = new JsonObject();
            sendContent.addProperty("topic", topic);
            sendContent.addProperty("brokers", entry.getValue());
            sendContent.addProperty("allbrokers", brokers);
            sendContent.addProperty("leader", id);
            sendContent.addProperty("failed", failed);
            object.add("content", sendContent);

            System.out.println("rebuild edge: " + key);
            if (key.equals(id)) {
                rebuildEdges(object);
            } else {
                String ip = key.split(":")[0];
                int port = Integer.valueOf(key.split(":")[1]);
                try {
                    Socket socket = new Socket(ip, port);
                    PrintStream printStream = new PrintStream(socket.getOutputStream());
                    printStream.println(object.toString());
                    //printStream.close();
                    //socket.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }

    private void notifyAddEdge(String topic, List<String[]> pairs) {
        System.out.println("notify other servers to add" + pairs.size() + " edges");
        for (String[] pair : pairs) {
            String server1 = pair[0];
            String server2 = pair[1];
            sendEdge(topic, server1, server2);
            sendEdge(topic, server2, server1);
        }
    }

    /**
     * tell server1 that it connect with server2 on specific topic
     *
     * @param topic
     * @param server1
     * @param server2
     */
    private void sendEdge(String topic, String server1, String server2) {
        System.out.println("sendEdge is called.");

        JsonObject message = new JsonObject();
        message.addProperty("sender", id);
        message.addProperty("action", MessageAction.ADD_EDGE);
        JsonObject content = new JsonObject();
        content.addProperty("topic", topic);
        content.addProperty("server", server2);
        //content.put("leader", leaderMap.get(topic));
        message.add("content", content);
        if (server1.equals(id)) {
            addEdge(message);
            return;
        }

        String ip = server1.split(":")[0];
        int port = Integer.valueOf(server1.split(":")[1]);
        try {
            Socket socket = new Socket(ip, port);
            //socket.setSoTimeout(1000);

            PrintStream printStream = new PrintStream(socket.getOutputStream());
            printStream.println(message.toString());
            //printStream.flush();
            //printStream.close();
            //socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * @param topic   which topic
     * @param message {sender:___,
     *                action:____,
     *                content:____}
     */
    private void routingServer(String topic, JsonObject message) {
        System.out.println(topic + ": routingServer is called.");
        System.out.println(message.toString());
        if (!routingMap.containsKey(topic)) {
            System.out.println("No server to routing");
            return;
        }
        Set<String> serverSet = routingMap.get(topic);
        if (serverSet == null) return;
        Socket socket;
        String sender = message.get("sender").getAsString();
        message.addProperty("sender", id);
        List<String> failed = new ArrayList<>();
        for (String serverID : serverSet) {
            if (serverID.equals(sender)) {
                continue;
            }
            String[] server = serverID.split(":");
            String ip = server[0];
            int port = Integer.valueOf(server[1]);
            try {
                socket = new Socket(ip, port);
                //socket.setSoTimeout(1000);

                PrintStream printStream = new PrintStream(socket.getOutputStream());
                printStream.println(message.toString());
                printStream.flush();
                //printStream.close();
                //socket.close();
            } catch (IOException e) {
                failed.add(serverID);
            }

        }
        if (failed.size() > 0) {
            sendBrokerDown(failed);
        }
    }

    /**
     * @param faileds
     */
    private void sendBrokerDown(List<String> faileds) {
        JsonObject object = new JsonObject();
        object.addProperty("sender", id);
        object.addProperty("action", MessageAction.SERVER_FAIL);
        StringBuilder sb = new StringBuilder();
        for (String failed : faileds) {
            sb.append(failed + ",");
        }
        object.addProperty("content", sb.toString());
        try {
            Socket socket = new Socket(zookeeperIp, zookeeperPort);
            //socket.setSoTimeout(1000);
            PrintStream printStream = new PrintStream(socket.getOutputStream());
            printStream.println(object.toString());
            //printStream.close();
            //socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * add server on topic. in a spanning tree.
     *
     * @param message
     */
    private void addEdge(JsonObject message) {
        System.out.println("addEdge is called.");
        JsonObject content = message.get("content").getAsJsonObject();
        String topic = content.get("topic").getAsString();
        String serverId = content.get("server").getAsString();
        String leader = message.get("sender").getAsString();

        leaderMap.put(topic, leader);
//        if (!topicCount.containsKey(topic)) {
//            topicCount.put(topic, new AtomicInteger(0));
//        }

        if (!routingMap.containsKey(topic)) {
            routingMap.put(topic, new HashSet<>());
        }
        routingMap.get(topic).add(serverId);

//        if(!topicServerMap.containsKey(topic)){
//            topicServerMap.put(topic,new HashSet<>());
//        }
//        topicServerMap.get(topic).add(serverId);
        //System.out.println(topic + ": edges：" + serverId + "added");
    }


    /**
     * add a new server to spanning tree
     *
     * @param message{sender: zookeeper,
     *                        action: ADD_NEW_BROKER,
     *                        content: {topic: topic,
     *                        broker: server}
     */
    private void addToSpanningTree(JsonObject message) {
        JsonObject content = message.get("content").getAsJsonObject();
        String topic = content.get("topic").getAsString();
        String serverID = content.get("broker").getAsString();

        Set<String> serverSet = topicServerMap.get(topic);
        System.out.println("group size: " + serverSet.size());
        int idx = (int) (Math.random() * serverSet.size());
        String needToAdd = null;
        for (String server : serverSet) {
            needToAdd = server;
            if (idx == 0) {
                break;
            }
            idx--;
        }

        topicServerMap.get(topic).add(serverID);
        //allOne.inc(serverID);

        System.out.println("Add " + serverID + " to " + needToAdd);
        sendEdge(topic, needToAdd, serverID);
        sendEdge(topic, serverID, needToAdd);

        try {
            Thread.sleep(500);
        } catch (Exception e) {
            e.printStackTrace();
        }
        sendSynchronizeData(serverID, topic);

        try {
            Thread.sleep(500);
        } catch (Exception e) {
            e.printStackTrace();
        }

        JsonObject addServerObject = new JsonObject();
        addServerObject.addProperty("sender", id);
        addServerObject.addProperty("action", MessageAction.ADD_SERVER);
        JsonObject sendContent = new JsonObject();
        sendContent.addProperty("topic", topic);
        sendContent.addProperty("server", serverID);
        addServerObject.add("content", sendContent);
        routingServer(topic, addServerObject);
    }

    /**
     * @param message{sender: zookeeper,
     *                        action: server failed,
     *                        content:{topic: topic,
     *                        broker: serverID}}
     */
    private void delFromSpanningTree(JsonObject message) {

    }

    /**
     * send synchronize data to another server
     *
     * @param serverID
     */
    private void sendSynchronizeData(String serverID, String topic) {
        JsonObject message = new JsonObject();

        Map<String, Set<String>> clientMap = new HashMap<>();
        for (String key : serverClientMap.keySet()) {
            if (key.endsWith(topic))
                clientMap.put(key, serverClientMap.get(key));
        }
        JsonObject serverClientObject = JsonUtil.mapToJson(clientMap);
        message.add("server_client_map", serverClientObject);

        Map<String, Set<String>> brokerGroup = new HashMap<>();
        brokerGroup.put(topic, topicServerMap.get(topic));
        JsonObject topicServerObject = JsonUtil.mapToJson(brokerGroup);
        message.add("topic_server", topicServerObject);

        Map<String, Integer> load = new HashMap<>();
        for (String broker : serverLoad.keySet()) {
            if (topicServerMap.get(topic).contains(broker))
                load.put(broker, serverLoad.get(broker));
        }
        JsonObject serverLoadObject = JsonUtil.loadToJson(load);
        message.add("server_load", serverLoadObject);


        //JsonObject serverClientObject = JsonUtil.mapToJson(serverClientMap);
        //JsonObject topicServerObject = JsonUtil.mapToJson(topicServerMap);
        //JsonObject serverLoadObject = JsonUtil.loadToJson(serverLoad);
        //JsonObject message = new JsonObject();
        message.addProperty("sender", id);
        message.addProperty("action", MessageAction.SYNCHRONIZE);
        //message.put("server_client_map", serverClientObject);
        //message.put("topic_server", topicServerObject);
        //message.put("server_load", serverLoadObject);
        try {
            String[] server = serverID.split(":");
            String ip = server[0];
            int port = Integer.valueOf(server[1]);

            Socket socket = new Socket(ip, port);
            PrintStream printStream = new PrintStream(socket.getOutputStream());
            printStream.println(message.toString());
            printStream.flush();
            //printStream.close();
            //socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * synchronize data from leader.
     *
     * @param message
     */
    private void synchronizedData(JsonObject message) {
        JsonObject serverClientObject = (JsonObject) message.get("server_client_map");
        serverClientMap = JsonUtil.jsonToMap(serverClientObject);
        JsonObject serverLoadObject = (JsonObject) message.get("server_load");
        serverLoad = JsonUtil.jsonToLoadMap(serverLoadObject);
//        JsonObject serverClientObject = (JsonObject) message.get("server_client_map");
        JsonObject topicServerObject = (JsonObject) message.get("topic_server");
//        JsonObject serverLoadObject = (JsonObject) message.get("server_load");
//        serverClientMap = JsonUtil.jsonToMap(serverClientObject);
        topicServerMap = JsonUtil.jsonToMap(topicServerObject);
//        serverLoad = JsonUtil.jsonToLoadMap(serverLoadObject);

        System.out.println("Synchronized data serverclientmap size: " + serverClientMap.size());
        System.out.println("Synchronized data topicServerMap size: " + topicServerMap.size());
        System.out.println("Synchronized data serverLoad size: " + serverLoad.size());
    }

    /**
     * Every server need to add server to its topic server map
     *
     * @param message{sender: zookeeper,
     *                        action: ADD_SERVER,
     *                        content: {topic: topic_name,
     *                        server: serverId}}
     */
    private void addServer(JsonObject message) {
        JsonObject content = message.get("content").getAsJsonObject();
        String topic = content.get("topic").getAsString();
        String serverId = content.get("server").getAsString();

        if (!topicServerMap.containsKey(topic)) {
            topicServerMap.put(topic, new HashSet<>());
        }
        topicServerMap.get(topic).add(serverId);
        routingServer(topic, message);
    }

    /**
     * Send message to clients.
     *
     * @param message
     */
    private void sendMsg(JsonObject message) {
        JsonObject content = message.get("content").getAsJsonObject();
        String topic = content.get("topic").getAsString();
        String sender = message.get("sender").getAsString();
        int num = Integer.valueOf(content.get("id").getAsString());
        System.out.println("message id:" + num);

        if (!topicCount.containsKey(topic)) {
            topicCount.put(topic, new AtomicInteger(0));
        }
        int curCount = topicCount.get(topic).get();
        topicCount.get(topic).set(num);

        String serverId = ip + ":" + port;
        String key = serverId + ":" + topic;
        Set<String> clientSet = serverClientMap.get(key);

        routingServer(topic, message);
        message.addProperty("sender", id);

        if (topicMsgMap.get(topic) == null) {
            topicMsgMap.put(topic, new ArrayList<>());
        }
        topicMsgMap.get(topic).add(num + ": " +content.toString());

        if (topicMsgMap.get(topic).size() == 10) {
            WriteToFileThread writeTofileThread = new WriteToFileThread(serverId, topic);
            writeTofileThread.run();
            topicMsgMap.get(topic).clear();
        }

        if (!(clientSet == null || clientSet.size() == 0)) {

            Socket socket;
            for (String c : clientSet) {
                String ip = c.split(":")[0];
                int port = Integer.valueOf(c.split(":")[1]);
                try {
                    socket = new Socket(ip, port);
                    socket.setSoTimeout(1000);

                    PrintStream printStream = new PrintStream(socket.getOutputStream());
                    printStream.println(message.toString());
                    printStream.flush();
                    //printStream.close();
                    //socket.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        if (!sender.equals(id) && curCount != num - 1)
            System.out.println("missing msg topic: " + topic + " from " + (curCount + 1) + " to " + (num - 1));
//       {
//            JsonObject request = new JsonObject();
//            request.put("sender", subscriberAddr + ":" + subscriberPort);
//            request.put("action", "MISSING_MESSAGE");
//            request.put("content",  wrongId + ":" + (id - 1) );
//            topicMap.put(topic, id);
//            try {
//                Socket socket = new Socket(serverID[0], Integer.valueOf(serverID[1]));
//                //send request
//                PrintStream out = new PrintStream(socket.getOutputStream());
//                out.println(request);
//
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            reportMissingMsg(topic,curCount,num);
//        }
    }

    //Todo 3: write to log
    private void writeToLog() {

    }


    /**
     * delete server
     *
     * @param message
     */
    private void deleteServer(JsonObject message) {
        JsonObject content = message.get("content").getAsJsonObject();
        String topic = content.get("topic").getAsString();
        String serverId = content.get("broker").getAsString();
        if (topicServerMap.containsKey(topic)) {
            topicServerMap.get(topic).remove(serverId);
        }
        String key = serverId + ":" + topic;
        if (serverClientMap.containsKey(key)) {
            serverClientMap.remove(key);
        }

    }


    /**
     * register broker on zookeeper
     *
     * @param ip
     * @param port
     * @return
     */

    public boolean register(String ip, int port, String topics) {
//        StringBuilder sb = new StringBuilder();
//        for(String topic : topics){
//            sb.append(topic + ",");
//        }
        JsonObject object = new JsonObject();
        object.addProperty("sender", id);
        object.addProperty("action", MessageAction.BROKER_REG);
        object.addProperty("content", topics);
        //boolean success = false;
        try {
            Socket socket = new Socket(ip, port);
            PrintStream printStream = new PrintStream(socket.getOutputStream());
            printStream.println(object.toString());
            printStream.flush();
            //printStream.close();
//            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
//            StringBuilder sb = new StringBuilder();
//            String line;
//            while((line = bufferedReader.readLine()) != null){
//                sb.append(line);
//            }
//            JsonObject message = (JsonObject)parser.parse(sb.toString());
//            String action = (String)message.get("action");
//            if(action.equals(MessageAction.OK)){
//                success = true;
//            }
            //socket.close();
        } catch (UnknownHostException e) {
            System.out.println("Unknown host!");
        } catch (ConnectException e) {
            System.out.println("Connection Error! Please check wether zookeeper is alive!");
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("IO Error!");
        }
//        catch (ParseException e){
//            System.out.println("Message received is not Json Object!");
//        }
        return true;
    }


    private void brokerFailed(JsonObject message) {
        JsonObject content = (JsonObject) message.get("content");
        String topic = content.get("topic").getAsString();
        String broker = content.get("broker").getAsString();

        String key = broker + ":" + topic;
        Set<String> clients = serverClientMap.get(key);
//        JsonObject delServerObject = new JsonObject();
//        delServerObject.put("sender", id);
//        delServerObject.put("action", MessageAction.DEL_SERVER);
//        delServerObject.put("content", content);
//        deleteServer(delServerObject);
        if (topicServerMap.containsKey(topic)) {
            topicServerMap.get(topic).remove(broker);
        }
        if (serverClientMap.containsKey(key)) {
            serverClientMap.remove(key);
        }
        if (serverLoad.containsKey(broker))
            serverLoad.remove(broker);

        JsonObject buildObject = new JsonObject();
        buildObject.addProperty("sender", id);
        buildObject.addProperty("action", MessageAction.BUILD_SPANNING_TREE);
        JsonObject buildContent = new JsonObject();
        Set<String> servers = topicServerMap.get(topic);
        buildContent.addProperty("topic", topic);
        StringBuilder sb = new StringBuilder();
        for (String server : servers) {
            sb.append(server + ",");
        }
        buildContent.addProperty("brokers", sb.toString());
        buildContent.addProperty("failed", broker);
        buildObject.add("content", buildContent);
        buildSpanningTree(buildObject);

        if (clients != null) {
            for (String client : clients) {
                JsonObject alloMsg = new JsonObject();
                JsonObject alloMsgContent = new JsonObject();
                alloMsgContent.addProperty("topic", topic);
                alloMsgContent.addProperty("client", client);
                alloMsg.add("content", alloMsgContent);
                allocateClient(alloMsg);
            }
        }
    }

    private void refreshEdges(String topic, List<String[]> pairs) {
        Map<String, Set<String>> graph = new HashMap<>();
        for (String[] pair : pairs) {
            graph.putIfAbsent(pair[0], new HashSet<>());
            graph.get(pair[0]).add(pair[1]);
            graph.putIfAbsent(pair[1], new HashSet<>());
            graph.get(pair[1]).add(pair[0]);
        }

        for (Map.Entry<String, Set<String>> entry : graph.entrySet()) {
            String key = entry.getKey();
            Set<String> servers = entry.getValue();
            StringBuilder sb = new StringBuilder();
            for (String server : servers) {
                sb.append(server + ",");
            }
            sb.deleteCharAt(sb.length() - 1);

            JsonObject object = new JsonObject();
            object.addProperty("sender", id);
            object.addProperty("action", MessageAction.REBUILD_EDGES);
            JsonObject content = new JsonObject();
            content.addProperty("topic", topic);
            content.addProperty("brokers", sb.toString());


            Set<String> allServers = graph.keySet();
            sb = new StringBuilder();
            for (String server : allServers) {
                sb.append(server + ",");
            }
            sb.deleteCharAt(sb.length() - 1);
            content.addProperty("allbrokers", sb.toString());
            object.add("content", content);
            System.out.println("rebuild edge: " + key);
            if (key.equals(id)) {
                rebuildEdges(object);
                continue;
            }

            String ip = key.split(":")[0];
            int port = Integer.valueOf(key.split(":")[1]);
            try {
                Socket socket = new Socket(ip, port);
                PrintStream printStream = new PrintStream(socket.getOutputStream());
                printStream.println(object.toString());
                printStream.close();
                socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void rebuildEdges(JsonObject message) {
        JsonObject content = message.get("content").getAsJsonObject();
        String topic = content.get("topic").getAsString();
        String leader = content.get("leader").getAsString();
        leaderMap.put(topic, leader);
        if (!topicCount.containsKey(topic)) {
            topicCount.put(topic, new AtomicInteger(0));
        }

        routingMap.remove(topic);
        routingMap.put(topic, new HashSet<>());
        String routing = content.get("brokers").getAsString();
        if (routing != null && !routing.equals("")) {
            String[] brokers = routing.split(",");
            for (String broker : brokers) {
                routingMap.get(topic).add(broker);
            }
        }

        topicServerMap.remove(topic);
        topicServerMap.put(topic, new HashSet<>());
        String group = content.get("allbrokers").getAsString();
        if (group != null && !group.equals("")) {
            String[] allBrokers = group.split(",");
            for (String broker : allBrokers) {
                topicServerMap.get(topic).add(broker);
            }
        }

        String failed = content.get("failed").getAsString();
        if (serverLoad.containsKey(failed))
            serverLoad.remove(failed);
        if (failed != null && !leader.equals(id)) {
            serverClientMap.remove(failed + ":" + topic);
        }
    }

    private class WriteToFileThread extends Thread {
        String serverId;
        String topic;

        public WriteToFileThread(String serverId, String topic) {
            this.serverId = serverId;
            this.topic = topic;
        }

        @Override
        public void run() {
            System.out.println("The message has reached 100 writing to file");
            try {
                String path = System.getProperty("user.dir") + "/broker/" + topic + serverId + "Message";
                File file = new File(path);
                System.out.println(file.getParentFile());
                if (!file.getParentFile().exists()) {
                    file.getParentFile().mkdirs();
//                    if (!result) {
//                        System.out.println("创建失败");
//                    }
                }
                FileWriter fileWriter = new FileWriter(path, true);
                BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
                PrintWriter out = new PrintWriter(bufferedWriter);
                for (String s : topicMsgMap.get(topic)) {
                        out.println(s);
                    }
                out.close();
                bufferedWriter.close();
                fileWriter.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }


//    private void missMsgHandler(JsonObject message) {
//        System.out.println("Detect message missing");
//        String clientInfo = (String)message.get("sender");
//
//        JsonObject content = (JsonObject) message.get("content");
//        String[] msgRange = content.toString().split(":");
//        int from = Integer.valueOf(msgRange[0]);
//        int to = Integer.valueOf(msgRange[1]);
//        StringBuilder sb = new StringBuilder();
//
//        if ((to - from) >=10) {
//            String filePath =  "/Users/xiaopu/IdeaProjects/publisherSubscriber/src/main/resources/broker" + ip + ":"+ port + "Message";;
//            Scanner sc = new Scanner(filePath);
//            while (sc.hasNextLine()) {
//                String msg = sc.nextLine();
//                String[] msgKV = msg.split(":");
//                int n = Integer.valueOf(msgKV[0]);
//                if ((n >= from) && (n <= to)) {
//                    sb.append(msg);
//                }
//            }
//        }
//        System.out.println(sb.toString());
//
//        JsonObject object = new JsonObject();
//        object.addProperty("sender", clientInfo);
//        object.addProperty("action", MessageAction.SEND_MESSAGE);
//        object.addProperty("content", sb.toString());
//
//        SendMissingMegThread sendMissingMegThread = new SendMissingMegThread(object, clientInfo);
//        sendMissingMegThread.run();
//    }
//
//    private class SendMissingMegThread extends Thread{
//        private String clientInfo;
//        private JsonObject message;
//
//        public SendMissingMegThread(JsonObject message, String clientInfo) {
//            this.message = message;
//            this.clientInfo = clientInfo;
//        }
//        @Override
//        public void run(){
//            try {
//                String[] infos = clientInfo.split(":");
//                String clientIP = infos[0];
//                int clientPort = Integer.valueOf(infos[1]);
//                Socket socket = new Socket(clientIP, clientPort);
//
//                PrintStream out = new PrintStream(socket.getOutputStream());
//                out.println(message.toString());
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }

    private void missMsgHandler(JsonObject message) {
        System.out.println("Detect message missing");
        String clientInfo = message.get("sender").getAsString();

        JsonObject content = (JsonObject) message.get("content");
        String[] msgRange = content.toString().split(":");
        int from = Integer.valueOf(msgRange[0]);
        int to = Integer.valueOf(msgRange[1]);

        String filePath = "/Users/xiaopu/IdeaProjects/publisherSubscriber/src/main/resources/broker" + ip + ":" + port + "Message";
        ;
        Scanner sc = new Scanner(filePath);
        StringBuilder sb = new StringBuilder();
        while (sc.hasNextLine()) {
            String msg = sc.nextLine();
            String[] msgKV = msg.split(":");
            int n = Integer.valueOf(msgKV[0]);
            if ((n >= from) && (n <= to)) {
                sb.append(msg);
            }
        }
        System.out.println(sb.toString());

        JsonObject object = new JsonObject();
        object.addProperty("sender", clientInfo);
        object.addProperty("action", MessageAction.SEND_MESSAGE);
        object.addProperty("content", sb.toString());

        SendMissingMegThread sendMissingMegThread = new SendMissingMegThread(object, clientInfo);
        sendMissingMegThread.run();
    }

    private class SendMissingMegThread extends Thread {
        private String clientInfo;
        private JsonObject message;

        public SendMissingMegThread(JsonObject message, String clientInfo) {
            this.message = message;
            this.clientInfo = clientInfo;
        }

        @Override
        public void run() {
            try {
                String[] infos = clientInfo.split(":");
                String clientIP = infos[0];
                int clientPort = Integer.valueOf(infos[1]);
                Socket socket = new Socket(clientIP, clientPort);

                PrintStream out = new PrintStream(socket.getOutputStream());
                out.println(message.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * inner thread class. Responsible to handle all kinds of actions.
     */
    private class Handler extends Thread {
        private Socket socket;

        Handler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            //System.out.println("handler running");
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                //StringBuilder sb = new StringBuilder();
                String inputLine = reader.readLine();
//                while((inputLine = reader.readLine()) != null){
//                    sb.append(inputLine);
//                }
                //socket.close();
                //System.out.println(inputLine);
                JsonObject object = parser.parse(inputLine).getAsJsonObject();
                String action = object.get("action").getAsString();
                System.out.println("Action: " + action);
                switch (action) {
                    case MessageAction.ADD_CLIENT:
                        addClient(object);
                        break;
                    case MessageAction.DEL_CLIENT:
                        delClient(object);
                        break;
                    case MessageAction.ADD_SERVER:
                        addServer(object);
                        break;
//                    case MessageAction.DEL_SERVER:
//                        deleteServer(object);
//                        break;
                    case MessageAction.ALLOCATE_CLIENT:
                        allocateClient(object);
                        break;
                    case MessageAction.BUILD_SPANNING_TREE:
                        buildSpanningTree(object);
                        break;
                    case MessageAction.SEND_MESSAGE:
                        sendMsg(object);
                        break;
                    case MessageAction.ADD_EDGE:
                        addEdge(object);
                        break;
                    case MessageAction.ADD_NEW_BROKER:
                        addToSpanningTree(object);
                        break;
                    case MessageAction.SYNCHRONIZE:
                        synchronizedData(object);
                        break;
                    case MessageAction.NEW_FEED:
                        newFeed(object);
                        break;
                    case MessageAction.SERVER_FAIL:
                        brokerFailed(object);
                        break;
                    case MessageAction.REBUILD_EDGES:
                        rebuildEdges(object);
                        break;
                    case MessageAction.MISS_MESSAGE:
                        //missMsgHandler(object);
                        break;
                }

                reader.close();
                socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    public void run() {
        try {
            //System.out.println("Please input zookeeper IP address:");
            String zookeeperIp = "127.0.0.1";
            //System.out.println("Please input zookeeper port number: ");
            int zookeeperPort = 8889;
            ServerSocket serverSocket = new ServerSocket(port);
            System.out.println("Listening on port: " + port);
            Scanner scanner = new Scanner(System.in);
            System.out.println("Please input topics that brokers need to register: ");
            String topics = scanner.nextLine();
            setZookeeper(zookeeperIp, zookeeperPort);
            if (!register(zookeeperIp, zookeeperPort, topics)) {
                System.out.println("Register failed!");
            } else {
                System.out.println("Register succeed!");
            }

            ExecutorService threadPool = Executors.newFixedThreadPool(10);
            while (true) {
                Socket socket = serverSocket.accept();

                Handler handler = new Handler(socket);
                //handler.start();
                threadPool.execute(handler);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        int port = 10000;
        Broker broker = new Broker();
        Scanner scanner = new Scanner(System.in);
        boolean success = false;


        System.out.print("Please input port number: ");
        port = Integer.valueOf(scanner.nextLine());
        broker.setIPAndPort("127.0.0.1", port);

        broker.init();
        broker.run();
    }
}
