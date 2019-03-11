package broker;

import message.MessageAction;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import util.JsonUtil;

import java.io.*;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Broker {

    private String ip = "127.0.0.1";
    private int port;
    private JSONParser parser;
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
        parser = new JSONParser();
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
    private void newFeed(JSONObject message) {
        System.out.println("newFeed is called.");
        JSONObject content = (JSONObject) message.get("content");
        String topic = (String) content.get("topic");
        String messagecontent = (String) message.get("msg");

        int num = topicCount.get(topic).incrementAndGet();
        content.put("id", num + "");
        //content.put("message", messagecontent);
        JSONObject object = new JSONObject();
        object.put("sender", id);
        object.put("action", MessageAction.SEND_MESSAGE);
        object.put("content", content);

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
    private void addClient(JSONObject message) {
        System.out.println("addclient is called.");
        try {
            JSONObject content = (JSONObject) message.get("content");
            String serverId = (String) content.get("server");
            String clientId = (String) content.get("client");
            String topic = (String) content.get("topic");
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
    private int delClient(JSONObject message) {
        System.out.println("delClient is called.");
        try {
            //int count = 0;
            JSONObject content = (JSONObject) message.get("content");
            String topic = (String) content.get("topic");
            String clientId = (String) content.get("client");
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
    private void allocateClient(JSONObject message) {
        System.out.println("allocateClient is called.");
        try {
            JSONObject content = (JSONObject) message.get("content");
            String topic = (String) content.get("topic");
            String clientId = (String) content.get("client");
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


            JSONObject addClient = new JSONObject();
            addClient.put("sender", id);
            addClient.put("action", "ADD_CLIENT");
            JSONObject sendContent = new JSONObject();
            sendContent.put("topic", topic);
            sendContent.put("server", minLoad);
            sendContent.put("client", clientId);
            addClient.put("content", sendContent);
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
    private void buildSpanningTree(JSONObject message) {
        System.out.println("buildSpanningTree is called.");
        JSONObject content = (JSONObject) message.get("content");
        String topic = (String) content.get("topic");
        String failed = (String) content.get("failed");

        leaderMap.put(topic, id);
        topicServerMap.put(topic, new HashSet<>());

        if (!topicCount.containsKey(topic)) {
            topicCount.put(topic, new AtomicInteger(0));
        }

        String brokers = (String) content.get("brokers");
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
            JSONObject object = new JSONObject();
            object.put("sender", id);
            object.put("action", MessageAction.REBUILD_EDGES);
            JSONObject sendContent = new JSONObject();
            sendContent.put("topic", topic);
            sendContent.put("brokers", entry.getValue());
            sendContent.put("allbrokers", brokers);
            sendContent.put("leader", id);
            sendContent.put("failed", failed);
            object.put("content", sendContent);

            System.out.println("rebuild edge: " + key);
            if (key.equals(id)) {
                rebuildEdges(object);
            } else {
                String ip = key.split(":")[0];
                int port = Integer.valueOf(key.split(":")[1]);
                try {
                    Socket socket = new Socket(ip, port);
                    PrintStream printStream = new PrintStream(socket.getOutputStream());
                    printStream.println(object.toJSONString());
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

        JSONObject message = new JSONObject();
        message.put("sender", id);
        message.put("action", MessageAction.ADD_EDGE);
        JSONObject content = new JSONObject();
        content.put("topic", topic);
        content.put("server", server2);
        //content.put("leader", leaderMap.get(topic));
        message.put("content", content);
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
            printStream.println(message.toJSONString());
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
    private void routingServer(String topic, JSONObject message) {
        System.out.println(topic + ": routingServer is called.");
        System.out.println(message.toJSONString());
        if (!routingMap.containsKey(topic)) {
            System.out.println("No server to routing");
            return;
        }
        Set<String> serverSet = routingMap.get(topic);
        if (serverSet == null) return;
        Socket socket;
        String sender = (String) message.get("sender");
        message.put("sender", id);
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
                printStream.println(message.toJSONString());
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
        JSONObject object = new JSONObject();
        object.put("sender", id);
        object.put("action", MessageAction.SERVER_FAIL);
        StringBuilder sb = new StringBuilder();
        for (String failed : faileds) {
            sb.append(failed + ",");
        }
        object.put("content", sb.toString());
        try {
            Socket socket = new Socket(zookeeperIp, zookeeperPort);
            //socket.setSoTimeout(1000);
            PrintStream printStream = new PrintStream(socket.getOutputStream());
            printStream.println(object.toJSONString());
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
    private void addEdge(JSONObject message) {
        System.out.println("addEdge is called.");
        JSONObject content = (JSONObject) message.get("content");
        String topic = (String) content.get("topic");
        String serverId = (String) content.get("server");
        String leader = (String) message.get("sender");

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
    private void addToSpanningTree(JSONObject message) {
        JSONObject content = (JSONObject) message.get("content");
        String topic = (String) content.get("topic");
        String serverID = (String) content.get("broker");

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

        JSONObject addServerObject = new JSONObject();
        addServerObject.put("sender", id);
        addServerObject.put("action", MessageAction.ADD_SERVER);
        JSONObject sendContent = new JSONObject();
        sendContent.put("topic", topic);
        sendContent.put("server", serverID);
        addServerObject.put("content", sendContent);
        routingServer(topic, addServerObject);
    }

    /**
     * @param message{sender: zookeeper,
     *                        action: server failed,
     *                        content:{topic: topic,
     *                        broker: serverID}}
     */
    private void delFromSpanningTree(JSONObject message) {

    }

    /**
     * send synchronize data to another server
     *
     * @param serverID
     */
    private void sendSynchronizeData(String serverID, String topic) {
        JSONObject message = new JSONObject();

        Map<String, Set<String>> clientMap = new HashMap<>();
        for (String key : serverClientMap.keySet()) {
            if (key.endsWith(topic))
                clientMap.put(key, serverClientMap.get(key));
        }
        if (!clientMap.isEmpty()) {
            JSONObject serverClientObject = JsonUtil.mapToJson(clientMap);
            message.put("server_client_map", serverClientObject);
        } else {
            message.put("server_client_map", "");
        }

        Map<String, Set<String>> brokerGroup = new HashMap<>();
        brokerGroup.put(topic, topicServerMap.get(topic));
        JSONObject topicServerObject = JsonUtil.mapToJson(brokerGroup);
        message.put("topic_server", topicServerObject);

        Map<String, Integer> load = new HashMap<>();
        for (String broker : serverLoad.keySet()) {
            if (topicServerMap.get(topic).contains(broker))
                load.put(broker, serverLoad.get(broker));
        }
        if (!load.isEmpty()) {
            JSONObject serverLoadObject = JsonUtil.loadToJson(load);
            message.put("server_load", serverLoadObject);
        } else {
            message.put("server_load", "");
        }

        //JSONObject serverClientObject = JsonUtil.mapToJson(serverClientMap);
        //JSONObject topicServerObject = JsonUtil.mapToJson(topicServerMap);
        //JSONObject serverLoadObject = JsonUtil.loadToJson(serverLoad);
        //JSONObject message = new JSONObject();
        message.put("sender", id);
        message.put("action", MessageAction.SYNCHRONIZE);
        //message.put("server_client_map", serverClientObject);
        //message.put("topic_server", topicServerObject);
        //message.put("server_load", serverLoadObject);
        try {
            String[] server = serverID.split(":");
            String ip = server[0];
            int port = Integer.valueOf(server[1]);

            Socket socket = new Socket(ip, port);
            PrintStream printStream = new PrintStream(socket.getOutputStream());
            printStream.println(message.toJSONString());
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
    private void synchronizedData(JSONObject message) {
        if (!((String) message.get("server_client_map")).equals("")) {
            JSONObject serverClientObject = (JSONObject) message.get("server_client_map");
            serverClientMap = JsonUtil.jsonToMap(serverClientObject);
        }
        if (!((String) message.get("server_load")).equals("")) {
            JSONObject serverLoadObject = (JSONObject) message.get("server_load");
            serverLoad = JsonUtil.jsonToLoadMap(serverLoadObject);
        }
//        JSONObject serverClientObject = (JSONObject) message.get("server_client_map");
        JSONObject topicServerObject = (JSONObject) message.get("topic_server");
//        JSONObject serverLoadObject = (JSONObject) message.get("server_load");
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
    private void addServer(JSONObject message) {
        JSONObject content = (JSONObject) message.get("content");
        String topic = (String) content.get("topic");
        String serverId = (String) content.get("serverId");

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
    private void sendMsg(JSONObject message) {
        JSONObject content = (JSONObject) message.get("content");
        String topic = (String) content.get("topic");
        String sender = (String) message.get("sender");
        int num = Integer.valueOf((String) content.get("id"));
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
        message.put("sender", id);

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
                    printStream.println(message.toJSONString());
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
//            JSONObject request = new JSONObject();
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
    private void deleteServer(JSONObject message) {
        JSONObject content = (JSONObject) message.get("content");
        String topic = (String) content.get("topic");
        String serverId = (String) content.get("broker");
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
        JSONObject object = new JSONObject();
        object.put("sender", id);
        object.put("action", MessageAction.BROKER_REG);
        object.put("content", topics);
        //boolean success = false;
        try {
            Socket socket = new Socket(ip, port);
            PrintStream printStream = new PrintStream(socket.getOutputStream());
            printStream.println(object.toJSONString());
            printStream.flush();
            //printStream.close();
//            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
//            StringBuilder sb = new StringBuilder();
//            String line;
//            while((line = bufferedReader.readLine()) != null){
//                sb.append(line);
//            }
//            JSONObject message = (JSONObject)parser.parse(sb.toString());
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


    private void brokerFailed(JSONObject message) {
        JSONObject content = (JSONObject) message.get("content");
        String topic = (String) content.get("topic");
        String broker = (String) content.get("broker");

        String key = broker + ":" + topic;
        Set<String> clients = serverClientMap.get(key);
//        JSONObject delServerObject = new JSONObject();
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

        JSONObject buildObject = new JSONObject();
        buildObject.put("sender", id);
        buildObject.put("action", MessageAction.BUILD_SPANNING_TREE);
        JSONObject buildContent = new JSONObject();
        Set<String> servers = topicServerMap.get(topic);
        buildContent.put("topic", topic);
        StringBuilder sb = new StringBuilder();
        for (String server : servers) {
            sb.append(server + ",");
        }
        buildContent.put("brokers", sb.toString());
        buildContent.put("failed", broker);
        buildObject.put("content", buildContent);
        buildSpanningTree(buildObject);

        if (clients != null) {
            for (String client : clients) {
                JSONObject alloMsg = new JSONObject();
                JSONObject alloMsgContent = new JSONObject();
                alloMsgContent.put("topic", topic);
                alloMsgContent.put("client", client);
                alloMsg.put("content", alloMsgContent);
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

            JSONObject object = new JSONObject();
            object.put("sender", id);
            object.put("action", MessageAction.REBUILD_EDGES);
            JSONObject content = new JSONObject();
            content.put("topic", topic);
            content.put("brokers", sb.toString());


            Set<String> allServers = graph.keySet();
            sb = new StringBuilder();
            for (String server : allServers) {
                sb.append(server + ",");
            }
            sb.deleteCharAt(sb.length() - 1);
            content.put("allbrokers", sb.toString());
            object.put("content", content);
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
                printStream.println(object.toJSONString());
                printStream.close();
                socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void rebuildEdges(JSONObject message) {
        JSONObject content = (JSONObject) message.get("content");
        String topic = (String) content.get("topic");
        String leader = (String) content.get("leader");
        leaderMap.put(topic, leader);
        if (!topicCount.containsKey(topic)) {
            topicCount.put(topic, new AtomicInteger(0));
        }

        routingMap.remove(topic);
        routingMap.put(topic, new HashSet<>());
        String routing = (String) content.get("brokers");
        if (routing != null && !routing.equals("")) {
            String[] brokers = routing.split(",");
            for (String broker : brokers) {
                routingMap.get(topic).add(broker);
            }
        }

        topicServerMap.remove(topic);
        topicServerMap.put(topic, new HashSet<>());
        String group = (String) content.get("allbrokers");
        if (group != null && !group.equals("")) {
            String[] allBrokers = group.split(",");
            for (String broker : allBrokers) {
                topicServerMap.get(topic).add(broker);
            }
        }

        String failed = (String) content.get("failed");
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
                String path = "/Users/xiaopu/IdeaProjects/publisherSubscriber/src/main/resources/broker" + topic + serverId + "Message";
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


//    private void missMsgHandler(JSONObject message) {
//        System.out.println("Detect message missing");
//        String clientInfo = (String)message.get("sender");
//
//        JSONObject content = (JSONObject) message.get("content");
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
//        JSONObject object = new JSONObject();
//        object.put("sender", clientInfo);
//        object.put("action", MessageAction.SEND_MESSAGE);
//        object.put("content", sb.toString());
//
//        SendMissingMegThread sendMissingMegThread = new SendMissingMegThread(object, clientInfo);
//        sendMissingMegThread.run();
//    }
//
//    private class SendMissingMegThread extends Thread{
//        private String clientInfo;
//        private JSONObject message;
//
//        public SendMissingMegThread(JSONObject message, String clientInfo) {
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

    private void missMsgHandler(JSONObject message) {
        System.out.println("Detect message missing");
        String clientInfo = (String) message.get("sender");

        JSONObject content = (JSONObject) message.get("content");
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

        JSONObject object = new JSONObject();
        object.put("sender", clientInfo);
        object.put("action", MessageAction.SEND_MESSAGE);
        object.put("content", sb.toString());

        SendMissingMegThread sendMissingMegThread = new SendMissingMegThread(object, clientInfo);
        sendMissingMegThread.run();
    }

    private class SendMissingMegThread extends Thread {
        private String clientInfo;
        private JSONObject message;

        public SendMissingMegThread(JSONObject message, String clientInfo) {
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
                System.out.println(inputLine);
                JSONObject object = (JSONObject) parser.parse(inputLine);
                String action = (String) object.get("action");
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
