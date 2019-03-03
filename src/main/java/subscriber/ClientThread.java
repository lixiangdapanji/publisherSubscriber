//package subscriber;
//import message.Message;
//import message.MessageAction;
//
//import java.io.ObjectInputStream;
//import java.io.ObjectOutputStream;
//import java.net.Socket;
//import java.util.List;
//
//public class ClientThread extends Thread {
//    private static final String RECEIVE_MESSAGE = " received";
//    private Socket socket;
//    public ClientThread(Socket socket) {
//        this.socket = socket;
//    }
//    public Socket getSocket() {
//        return socket;
//    }
//
//    @Override
//    public void run() {
//        try {
//            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
//            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
//            Message m = (Message) ois.readObject();
//            if(m.getAction().equals(MessageAction.NEW_FEED)) {
//                for (Feed f : (List<Feed>)m.getContent()){
//                    System.out.println("Client received new feed's id: " + f.getId() + " " + f.getType());
//                }
//                Message m2 = new Message(MessageSender.CLIENT, MessageAction.OK, RECEIVE_MESSAGE);
//                oos.writeObject(m2);
//            }
//            oos.close();
//            ois.close();
//            this.socket.close();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//}
