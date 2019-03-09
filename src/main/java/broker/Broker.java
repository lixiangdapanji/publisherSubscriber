package broker;

import message.MessageAction;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import util.AllOne;
import util.JsonUtil;


import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Broker {

    private String ip = "127.0.0.1";
    private int port ;
    private JSONParser parser;
    private String id;
    private AllOne allOne;
    private String zookeeperIp;
    private int zookeeperPort;

    //server -> client list map; Key: server ip addr:port:topic; value: client list(client: ip:port)
    private Map<String, Set<String>> serverClientMap;

    private Map<String, Set<String>> topicServerMap;

    //
    private Map<String, Set<String>> routingMap;

    //给message编号
    private AtomicInteger msgCount;

    public Broker(){
    }

    public Broker(String ip, int port) {
        setIPAndPort(ip, port);
        init();
    }

    public void init(){
        serverClientMap = new HashMap<>();
        topicServerMap = new HashMap<>();
        routingMap = new HashMap<>();
        msgCount = new AtomicInteger(0);
        parser = new JSONParser();
        allOne = new AllOne();
    }

    public void setIPAndPort(String ip, int port){
        this.ip = ip;
        this.port = port;
        id = ip + ":" + port;
    }

    public void setZookeeper(String ip, int port){
        this.zookeeperIp = ip;
        this.zookeeperPort = port;
    }

    /**
     * leader receive new feed. Add count to this message and routing server to send message to all clients.
     * @param message{sender: zookeeper,
     *               action: NEW_FEED,
     *               content:{topic:topic,
     *                        message: message}}
     */
    private void newFeed(JSONObject message){
        System.out.println("newFeed is called.");
        JSONObject content = (JSONObject) message.get("content");
        String topic = (String)content.get("topic");
        String messagecontent = (String) message.get("msg");

        int num = msgCount.getAndIncrement();
        content.put("id", num);
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
     * @param message {sender: broker,
     *                action: ADD_CLIENT,
     *                content:{topic: topic
     *                         server: ip:port,
     *                         client: ip:port}}
     */
    private void addClient(JSONObject message) {
        System.out.println("addclient is called.");
        try {
            JSONObject content = (JSONObject) message.get("content");
            String serverId = (String)content.get("server");
            String clientId = (String)content.get("client");
            String topic = (String)content.get("topic");
            String key = serverId + ":" + topic;
            if(!serverClientMap.containsKey(key)){
                serverClientMap.put(key, new HashSet<>());
            }
            serverClientMap.get(key).add(clientId);
            allOne.inc(serverId);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * delete client
     * @param message{sender: broker,
     *                action: DEL_CLIENT,
     *                content:{topic: topic,
     *                         client: ip:port}}
     * @return count of changes. -1:exception.
     */
    private int delClient(JSONObject message) {
        System.out.println("delClient is called.");
        try{
            int count = 0;
            JSONObject content = (JSONObject) message.get("content");
            String topic = (String)content.get("topic");
            String clientId = (String)content.get("client");
            Set<String> serverSet = topicServerMap.get(topic);
            for(String serverId : serverSet){
                String key = serverId + ":" + topic;
                if(serverClientMap.containsKey("key") && serverClientMap.get(key).contains(clientId)){
                    serverClientMap.get(key).remove(clientId);
                    count++;
                }
            }
            return count;
        }catch (Exception e){
            e.printStackTrace();
            return -1;
        }
    }

    /**
     * load balancing choosing a server to control this client.
     * @param message {sender: zookeeper,
     *                action: ALLOCATE_CLIENT,
     *                content:{topic: topic 1,
     *                          client: clientId}}
     */
    private void allocateClient(JSONObject message) {
        System.out.println("allocateClient is called.");
        try {
            JSONObject content = (JSONObject) message.get("content");
            String topic = (String)content.get("topic");
            String clientId = (String)content.get("client");
            String minLoad = allOne.getMinKey();

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
            routingServer(topic, addClient);
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    /**
     * build spanning tree
     * @param message{sender: zookeeper,
     *               action: BUILD_SPANNING_TREE,
     *               content:{topic: topic,
     *                        brokers: "server1, server2"}
     */
    private void buildSpanningTree(JSONObject message) {
        System.out.println("buildSpanningTree is called.");
        JSONObject content = (JSONObject)message.get("content");
        String topic = (String)content.get("topic");
        String brokers = (String)content.get("brokers");
        String[] brokerlist = brokers.split(",");

        for(String broker : brokerlist){
            allOne.inc(broker);
        }

        List<Integer> red = new ArrayList<>();
        List<Integer> blue = new ArrayList<>();

        Random random = new Random();
        int n = brokerlist.length;
        //System.out.println("buildSpanningTree " + n + " brokers");
        for(int i = 0; i < n; i++){
            blue.add(i);
        }

        int[][] graph = new int[n][n];
        int start = random.nextInt(n);
        red.add(start);
        blue.remove(start);

        while(blue.size() > 0){
            int b = random.nextInt(blue.size());
            int r = random.nextInt(red.size());
            graph[red.get(r)][blue.get(b)] = 1;
            graph[blue.get(b)][red.get(r)] = 1;
            blue.remove(b);
        }

        List<String[]> pairs = new ArrayList<>();
        for(int i = 0; i < n; i++){
            for(int j = i + 1; j < n; j++){
                if(graph[i][j] == 1){
                    pairs.add(new String[]{brokerlist[i], brokerlist[j]});
                }
            }
        }

        for(int i = 0; i < n; i++){
            for(int j = 0; j < n; j++){
                System.out.print(graph[i][j] + " ");
            }
            System.out.println();
        }

        for(String[] pair : pairs){
            String server1 = pair[0];
            String server2 = pair[1];
            if(server1.equals(id)){
                if(!routingMap.containsKey(topic)){
                    routingMap.put(topic, new HashSet<>());
                }
                routingMap.get(topic).add(server2);
            }else if(server2.equals(id)){
                if(!routingMap.containsKey(topic)){
                    routingMap.put(topic, new HashSet<>());
                }
                routingMap.get(topic).add(server1);
            }else{
                sendEdge(topic, server1, server2);
                sendEdge(topic, server2, server1);
            }
        }

    }

    /**
     * tell server1 that it connect with server2 on specific topic
     * @param topic
     * @param server1
     * @param server2
     */
    private void sendEdge(String topic, String server1, String server2){
        System.out.println("sendEdge is called.");
        JSONObject message = new JSONObject();
        message.put("sender", id);
        message.put("action", MessageAction.ADD_EDGE);
        JSONObject content = new JSONObject();
        content.put("topic", topic);
        content.put("server", server2);
        message.put("content", content);


        String ip = server1.split(":")[0];
        int port = Integer.valueOf(server1.split(":")[1]);
        try {
            Socket socket = new Socket(ip, port);
            socket.setSoTimeout(1000);

            PrintStream printStream = new PrintStream(socket.getOutputStream());
            printStream.println(message.toJSONString());
            printStream.flush();
            printStream.close();
            socket.close();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    /**
     *
     * @param topic which topic
     * @param message {sender:___,
     *                action:____,
     *                content:____}
     */
    private void routingServer(String topic, JSONObject message) {
        System.out.println("routingServer is called.");
        if(!routingMap.containsKey(topic)){
            return;
        }
        Set<String> serverSet = routingMap.get(topic);
        Socket socket;
        String sender = (String)message.get("sender");
        List<String> failed = new ArrayList<>();
        for(String serverID : serverSet){
            if(serverID.equals(sender)){
                continue;
            }
            String[] server = serverID.split(":");
            String ip = server[0];
            int port = Integer.valueOf(server[1]);
            try {
                socket = new Socket(ip, port);
                socket.setSoTimeout(1000);

                PrintStream printStream = new PrintStream(socket.getOutputStream());
                printStream.println(message.toJSONString());
                printStream.flush();
                printStream.close();
                socket.close();
            }catch (IOException e){
                failed.add(serverID);
            }

        }
        if(failed.size() > 0){
            sendBrokerDown(failed);
        }

    }

    /**
     *
     * @param faileds
     */
    private void sendBrokerDown(List<String> faileds){
        JSONObject object = new JSONObject();
        object.put("sender", id);
        object.put("action", MessageAction.SERVER_FAIL);
        StringBuilder sb = new StringBuilder();
        for(String failed : faileds){
            sb.append(failed + ",");
        }
        object.put("content", sb.toString());
        try {
            Socket socket = new Socket(zookeeperIp, zookeeperPort);
            socket.setSoTimeout(1000);
            PrintStream printStream = new PrintStream(socket.getOutputStream());
            printStream.println(object.toJSONString());
            printStream.close();
            socket.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    /**
     *add server on topic. in a spanning tree.
     * @param message
     */
    private void addEdge(JSONObject message){
        System.out.println("addEdge is called.");
        JSONObject content = (JSONObject)message.get("content");
        String topic = (String)content.get("topic");
        String serverId = (String)content.get("server");
        if(!routingMap.containsKey(topic)){
            routingMap.put(topic, new HashSet<>());
        }
        routingMap.get(topic).add(serverId);
    }


    /**
     * add a new server to spanning tree
     * @param message{sender: zookeeper,
     *               action: ADD_NEW_BROKER,
     *               content: {topic: topic,
     *                         broker: server}
     */
    private void addToSpanningTree(JSONObject message){
        JSONObject content = (JSONObject) message.get("content");
        String topic = (String)content.get("topic");
        String serverID = (String)content.get("broker");

        Set<String> serverSet = topicServerMap.get(topic);
        int idx = (int) (Math.random() * serverSet.size());
        String needToAdd = null;
        for(String server : serverSet){
            needToAdd = server;
            if(idx == 0){
                break;
            }
            idx--;
        }

        sendEdge(topic, needToAdd, serverID);
        sendEdge(topic, serverID, needToAdd);

        sendSynchronizeData(serverID);

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
     *
     * @param message{sender: zookeeper,
     *               action: server failed,
     *               content:{topic: topic,
     *                        broker: serverID}}
     */
    private void delFromSpanningTree(JSONObject message){

    }

    /**
     * send synchronize data to another server
     * @param serverID
     */
    private void sendSynchronizeData(String serverID){
        JSONObject serverClientObject = JsonUtil.mapToJson(serverClientMap);
        JSONObject topicServerObject = JsonUtil.mapToJson(topicServerMap);

        JSONObject message = new JSONObject();
        message.put("sender", id);
        message.put("action", MessageAction.SYNCHRONIZE);
        message.put("server_client_map", serverClientObject);
        message.put("topic_server", topicServerObject);
        try {
            String[] server = serverID.split(":");
            String ip = server[0];
            int port = Integer.valueOf(server[1]);

            Socket socket = new Socket(ip, port);
            PrintStream printStream = new PrintStream(socket.getOutputStream());
            printStream.println(message.toJSONString());
            printStream.flush();
            printStream.close();
            socket.close();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    /**
     * synchronize data from leader.
     * @param message
     */
    private void synchronizedData(JSONObject message){
        JSONObject serverClientObject = (JSONObject)message.get("server_client_map");
        JSONObject topicServerObject = (JSONObject)message.get("topic_server");
        serverClientMap = JsonUtil.jsonToMap(serverClientObject);
        topicServerMap = JsonUtil.jsonToMap(topicServerObject);
    }

    /**
     * Every server need to add server to its topic server map
     * @param message{sender: zookeeper,
     *               action: ADD_SERVER,
     *               content: {topic: topic_name,
     *                         server: serverId}}
     */
    private void addServer(JSONObject message) {
        JSONObject content = (JSONObject) message.get("content");
        String topic = (String)content.get("topic");
        String serverId = (String)content.get("serverId");

        if(!topicServerMap.containsKey(topic)){
            topicServerMap.put(topic, new HashSet<>());
        }
        topicServerMap.get(topic).add(serverId);
        routingServer(topic, message);
    }

    /**
     *Send message to clients.
     * @param message
     */
    private void sendMsg(JSONObject message) {
        JSONObject content = (JSONObject) message.get("content");
        int num = (int)content.get("id");
        msgCount.set(num);
        String topic = (String)content.get("topic");
        String serverId = ip + ":" + port;
        String key = serverId + ":" + topic;
        Set<String> clientSet = serverClientMap.get(key);
        if(clientSet == null || clientSet.size() == 0){
            System.out.println("Currently no clients!");
            return;
        }

        Socket socket;
        for(String c : clientSet){
            String ip = c.split(":")[0];
            int port = Integer.valueOf(c.split(":")[1]);
            try {
                socket = new Socket(ip, port);
                socket.setSoTimeout(1000);

                PrintStream printStream = new PrintStream(socket.getOutputStream());
                printStream.println(message.toJSONString());
                printStream.flush();
                printStream.close();
                socket.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        routingServer(topic, message);
    }

    //Todo 3: write to log
    private void writeToLog() {

    }


    /**
     * delete server
     * @param message
     */
    private void deleteServer(JSONObject message){
        JSONObject content = (JSONObject) message.get("content");
        String topic = (String)content.get("topic");
        String serverId = (String)content.get("server");
        if(topicServerMap.containsKey(topic)){
            topicServerMap.get(topic).remove(serverId);
        }
        String key = serverId + ":" + topic;
        if(serverClientMap.containsKey(key)){
            serverClientMap.remove(key);
        }

    }

    private List<String> getTopic(String ip, int port) {
        List<String> list = new ArrayList<>();
        try {
            Socket socket = new Socket(ip, port);
            //send request
            JSONObject request = new JSONObject();
            request.put("sender", id);
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
                list.add(s);
            }
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return list;
    }


    /**
     * register broker on zookeeper
     * @param ip
     * @param port
     * @return
     */
    public  boolean register(String ip, int port, List<String> topics) {
        StringBuilder sb = new StringBuilder();
        for(String topic : topics){
            sb.append(topic + ",");
        }
        JSONObject object = new JSONObject();
        object.put("sender", id);
        object.put("action", MessageAction.BROKER_REG);
        object.put("content", sb.length() == 0 ? sb.toString() : "");
        //boolean success = false;
        try {
            Socket socket = new Socket(ip, port);
            PrintStream printStream = new PrintStream(socket.getOutputStream());
            printStream.println(object.toJSONString());
            printStream.flush();
            printStream.close();
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
            socket.close();
        }catch (UnknownHostException e){
            System.out.println("Unknown host!");
        }catch (ConnectException e){
            System.out.println("Connection Error! Please check wether zookeeper is alive!");
        }catch (IOException e){
            e.printStackTrace();
            System.out.println("IO Error!");
        }
//        catch (ParseException e){
//            System.out.println("Message received is not Json Object!");
//        }
        return true;
    }

    /**
     * inner thread class. Responsible to handle all kinds of actions.
     */
    private class Handler extends Thread{
        private Socket socket;

        Handler(Socket socket){
            this.socket = socket;
        }

        @Override
        public void run(){
            System.out.println("handler running");
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                //StringBuilder sb = new StringBuilder();
                String inputLine = reader.readLine();
//                while((inputLine = reader.readLine()) != null){
//                    sb.append(inputLine);
//                }
                socket.close();
                JSONObject object = (JSONObject) parser.parse(inputLine);
                String action = (String)object.get("action");
                System.out.println("Action: " + action);
                switch (action){
                    case MessageAction.ADD_CLIENT:
                        addClient(object);
                        break;
                    case MessageAction.DEL_CLIENT:
                        delClient(object);
                        break;
                    case MessageAction.ADD_SERVER:
                        addServer(object);
                        break;
                    case MessageAction.DEL_SERVER:
                        deleteServer(object);
                        break;
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
                        break;
                }

            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }

    public void run(){
        try {
            ServerSocket serverSocket = new ServerSocket(port);
            System.out.println("Listening on port: " + port);
            while (true){
                Socket socket = serverSocket.accept();
                System.out.println("accept");
                Handler handler = new Handler(socket);
                handler.run();
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public static void main(String[] args){
        int port = 10000;
        Broker broker = new Broker();
        Scanner scanner = new Scanner(System.in);
        boolean success = false;
        while(!success){
            System.out.print("Please input port number: ");
            port = scanner.nextInt();
            broker.setIPAndPort("127.0.0.1", port);
            //System.out.println("Please input zookeeper IP address:");
            String zookeeperIp = "127.0.0.1";
            //System.out.println("Please input zookeeper port number: ");
            int zookeeperPort = 8889;
            System.out.println("Please input topics that brokers need to register: ");
            List<String> topics = new ArrayList<>();
            while(scanner.hasNext()){
                topics.add(scanner.next());
            }

            broker.setZookeeper(zookeeperIp, zookeeperPort);
            if(!broker.register(zookeeperIp, zookeeperPort, topics)){
                System.out.println("Register failed!");
            }else{
                System.out.println("Register succeed!");
                success = true;
            }
        }
        broker.init();
        broker.run();
    }
}
