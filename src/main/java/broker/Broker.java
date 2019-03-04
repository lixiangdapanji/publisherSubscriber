package broker;

import jdk.nashorn.internal.runtime.ECMAException;
import message.MessageAction;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import javax.sql.rowset.serial.SerialBlob;
import javax.swing.plaf.RootPaneUI;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class Broker {

    private String ip = "127.0.0.1";
    private int port ;
    private JSONParser parser;
    private String id;

    //server -> client list map; Key: server ip addr:port:topic; value: client list(client: ip:port)
    private Map<String, Set<String>> serverClientMap;

    private Map<String, Set<String>> topicServerMap;

    //
    private Map<String, Set<String>> routingMap;

    //给message编号
    private int msgCount;
    public Broker(String ip, int port) {
        this.ip = ip;
        this.port = port;
        id = ip + ":" + port;
        serverClientMap = new HashMap<>();
        topicServerMap = new HashMap<>();
        routingMap = new HashMap<>();
        msgCount = 0;
        parser = new JSONParser();
    }

    /**
     * add client
     * @param message {sender: broker,
     *                action: ADD_CLIENT,
     *                content:{server: ip:port,
     *                         client: ip:port}}
     */
    private void addClient(JSONObject message) {
        try {
            JSONObject content = (JSONObject) message.get("content");
            String serverId = (String)content.get("server");
            String clientId = (String)content.get("client");
            if(!serverClientMap.containsKey(serverId)){
                serverClientMap.put(serverId, new HashSet<>());
            }
            serverClientMap.get(serverId).add(clientId);
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
     *                content:{topic: [topic 1, topic 2, topic 3],
     *                          client: clientId}}
     */
    private void allocateClient(JSONObject message) {

        //Todo: implement a load balancing algo.
        try {

        }catch (Exception e){
            e.printStackTrace();
        }
    }


    /**
     * build spanning tree
     * @param message
     */
    private void buildSpanningTree(JSONObject message) {
        JSONObject content = (JSONObject)message.get("content");
        String topic = (String)content.get("topic");
        String brokers = (String)content.get("brokers");
        String[] brokerlist = brokers.split(",");

        List<Integer> red = new ArrayList<>();
        List<Integer> blue = new ArrayList<>();

        Random random = new Random();
        int n = brokerlist.length;
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
        if(!routingMap.containsKey(topic)){
            return;
        }
        Set<String> serverSet = routingMap.get(topic);
        Socket socket;
        for(String serverID : serverSet){
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
            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }


    /**
     *add server on topic. in a spanning tree.
     * @param message
     */
    private void addEdge(JSONObject message){
        JSONObject content = (JSONObject)message.get("content");
        String topic = (String)content.get("topic");
        String serverId = (String)content.get("server");
        if(!routingMap.containsKey(topic)){
            routingMap.put(topic, new HashSet<>());
        }
        routingMap.get(topic).add(serverId);
    }

    private void allocateServer(JSONObject message){

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
        String topic = (String)content.get("topic");
        String serverId = ip + ":" + port;
        String key = serverId + ":" + topic;
        Set<String> clientSet = serverClientMap.get(key);

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
    }

    private void writeToLog() {

    }

    //Todo 1: addToSpanningtree. Add a new server to spanning tree.
    //Todo 2: delete server
    //Todo 3: write to log

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
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                StringBuilder sb = new StringBuilder();
                String inputLine;
                while((inputLine = reader.readLine()) != null){
                    sb.append(inputLine);
                }
                JSONObject object = (JSONObject) parser.parse(sb.toString());
                String action = (String)object.get("action");
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
                Handler handler = new Handler(socket);
                handler.run();
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public static void main(String[] args){
        int port = 8888;
        Scanner scanner = new Scanner(System.in);
        System.out.print("Please input port number: ");
        port = scanner.nextInt();
        Broker broker = new Broker("127.0.0.1", port);
        broker.run();
    }


}
