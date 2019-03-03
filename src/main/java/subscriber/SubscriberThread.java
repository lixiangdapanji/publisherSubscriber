package subscriber;
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.net.Socket;


public class SubscriberThread extends Thread {
    private Socket socket;
    public SubscriberThread(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        try {
            InputStreamReader ois = new InputStreamReader(socket.getInputStream());
            OutputStreamWriter oos = new OutputStreamWriter(socket.getOutputStream());

            String in = ois.toString();
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(in);
            JSONObject jsonObject = (JSONObject) obj;

            String topic = (String) jsonObject.get("topic");
            String message = (String) jsonObject.get("message");
            System.out.println(topic + " :" + message);

            String out = "topic" + ":" + topic + "," + "message" + " :" + message;
            oos.write(out);

            oos.close();
            ois.close();
            this.socket.close();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
