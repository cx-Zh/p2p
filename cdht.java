import java.io.IOException;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.*;
import java.util.Scanner;


/**
 *
 *Created by Cenxi Zhang on 18/5/2018
 */
public class cdht {

    private static int peer;
    private static int succ_1;
    private static int succ_2;
    private static int prede_1 = -1;
    private static int prede_2 = -1;
    private static boolean flag = true;
    private static String host = "localhost";
    private static final int GAP =5000;

    private static int nb_send = 0;
    private static int nb_rece1 = 0;
    private static int nb_rece2 = 0;
    public static int Flag_dead = 0;
    private static int deadp;
    private static int minnb;
    private static boolean Flag_tok = false;
    private static int temppred;


    public static void main(String[] args) throws Exception {

        peer = Integer.parseInt(args[0]);
        succ_1 = Integer.parseInt(args[1]);
        succ_2 = Integer.parseInt(args[2]);

        DatagramSocket socket = new DatagramSocket(50000+peer);
        final DatagramSocket sock = socket;
        int temp1 = peer;


        Thread UDPServer = new Thread(() -> {
            while (true) {

                DatagramPacket request = new DatagramPacket(new byte[1024], 1024);

                try {
                    sock.receive(request);
                    String request_msg = new String(request.getData(), 0, request.getLength());

                    InetAddress client_host = request.getAddress();
                    System.out.println(request_msg);
                    if (request_msg.contains("response")) {                        //________________________________________________________
                        request_msg = request_msg.replace(".","");
                        int sender1 = Integer.parseInt(request_msg.split(" ")[8]);
                        if(sender1==succ_1){
                            nb_rece1+=1;
                        }
                        if(sender1==succ_2){
                            nb_rece2+=1;
                        }

                        if (nb_rece1>nb_rece2){
                            minnb=nb_rece2;
                            deadp=succ_2;
                        }
                        else{
                            minnb=nb_rece1;
                            deadp=succ_1;
                        }
                        if (nb_send-minnb>4){
//                            System.out.println("Peer "+deadp+" is no longer alive.");
                            Flag_dead=1;
                            nb_send=0;
                            nb_rece1=0;
                            nb_rece2=0;
                        }
                    }//-------------------------------------------------------------------------------
                    if (request_msg.contains("request")) {
                        request_msg = request_msg.replace(".","");
                        int sender = Integer.parseInt(request_msg.split(" ")[8]);
                        if (flag) {
                            prede_2 = sender;
                        } else {
                            prede_1 = sender;
                        }
                        flag = !flag;
                        String message1 = "A ping response message was received from Peer " + temp1 + ".";
//                        nb_send += 1;
                        byte[] buf = message1.getBytes();
                        DatagramPacket response = new DatagramPacket(buf, buf.length, client_host, 50000+sender);
                        sock.send(response);

                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        UDPServer.start();

        Thread.sleep(GAP);



        Thread TCPClient = new Thread(() -> {
            try {

                ServerSocket server = new ServerSocket(50000+peer);
                while (true) {
                    Socket connection = server.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                    String message = in.readLine();

                    if (message.contains("response")) {
                        System.out.println(message);
                    } else if (message.startsWith("depart")) {

                        int depart = Integer.parseInt(message.split(" ")[1]);
                        int depart_succ1 = Integer.parseInt(message.split(" ")[2]);
                        int depart_succ2 = Integer.parseInt(message.split(" ")[3]);
                        if (succ_1 == depart) {
                            succ_1 = depart_succ1;
                            succ_2 = depart_succ2;
                        } else {

                            succ_2 = depart_succ1;
                        }
                        System.out.println("    Peer "+depart+" will depart from the network.\n" +
                                "    My first successor is now peer "+succ_1+".\n" +
                                "    My second successor is now peer "+succ_2+".");
                    }else if(message.startsWith("kill")){ //-------------------------------------------------
                        Flag_tok=true;
                        temppred=Integer.parseInt(message.split(" ")[1]);
                    } else if(message.startsWith("tokill")){
                        if (deadp==succ_1){
                        succ_1=Integer.parseInt(message.split(" ")[1]);
                        succ_2=Integer.parseInt(message.split(" ")[2]);
                        }else{
                            succ_2=Integer.parseInt(message.split(" ")[1]);
                        }
                        System.out.println("Peer "+deadp+" is no longer alive."+"    My first successor is now peer "+succ_1+".\n" +
                                "    My second successor is now peer "+succ_2+".");
                    } else {
                        int file = Integer.parseInt(message.split(" ")[1]);
                        int prev = Integer.parseInt(message.split(" ")[3]);

                        if ((file%256 < succ_1) || peer > succ_1) {
                            System.out.println("    File " + file + " is here.\n" +
                                    "    A response message, destined for peer " + prev + ", has been sent.");
                            Socket s = new Socket(host, 50000 + prev);
                            String reply = "    Received a response message from peer " + peer + ", " +
                                    "which has the file " + file + ".";
                            DataOutputStream out = new DataOutputStream(s.getOutputStream());
                            out.writeBytes(reply);
                            s.close();
                        } else {
                            System.out.println("    File " + file + " is not stored here.\n" +
                                    "    File request message has been forwarded to my successor.");
                            Socket s = new Socket(host, 50000 + succ_1);
                            DataOutputStream out = new DataOutputStream(s.getOutputStream());
                            out.writeBytes(message);
                            s.close();
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        TCPClient.start();

        Thread TCPServer = new Thread(() -> {
            while (true) {
                if (Flag_dead==1){//----------------------------------------------------
                    System.out.println("      Peer "+deadp+" is no longer alive.");
                    try{
                        Socket s = new Socket(host, 50000 + succ_1);
                        if (deadp==succ_1) {
                            s = new Socket(host, 50000 + succ_2);}
                        String message = "kill" + " " + peer;
                        DataOutputStream out = new DataOutputStream(s.getOutputStream());
                        out.writeBytes(message);
                        s.close();
                        Flag_dead=0;
                    }catch (IOException e) {
                        e.printStackTrace();
                    }
                }//---------------------------------------------------------------------------
                if (Flag_tok){
                    try{
                        if (deadp==succ_2) {
                            Socket s = new Socket(host, 50000 + temppred);
                            String message = "tokill" + " " + peer+" "+succ_1;
                            DataOutputStream out = new DataOutputStream(s.getOutputStream());
                            out.writeBytes(message);
                            s.close();
                        }
                        Flag_tok=false;

                    }catch (IOException e) {
                        e.printStackTrace();
                    }
                }

                // Create a scanner to read from STDIN
                Scanner scanner = new Scanner(System.in);
                String line = scanner.nextLine();


                if (line.startsWith("request")) {
                    int file = Integer.parseInt(line.split(" ")[1]);
                    try {
                        Socket s = new Socket(host, 50000+succ_1);
                        String message = line + " original " + peer;
                        DataOutputStream out = new DataOutputStream(s.getOutputStream());
                        out.writeBytes(message);
                        s.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    System.out.println("    File request message for " + file + " has been sent to my successor.");

                } else if (line.contains("quit")) {
                    int pred1 = prede_1;
                    int pred2 = prede_2;

                    if (pred1 > pred2) {
                        int temp = pred2;
                        pred2 = pred1;
                        pred1 = temp;
                        prede_1 = pred1;
                        prede_2 = pred2;
                    }
                    try {
                        Socket s = new Socket(host, 50000+ prede_1);
                        String message = "depart"+" "+peer+" "+succ_1+" "+succ_2;
                        DataOutputStream out = new DataOutputStream(s.getOutputStream());
                        out.writeBytes(message);
                        s.close();

                        s = new Socket(host, 50000 + prede_2);
                        out = new DataOutputStream(s.getOutputStream());
                        out.writeBytes(message);
                        s.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    System.exit(0);
                }

//                if(nb_send-nb_rece>4){
//
//                }
            }
        });
        TCPServer.start();


        while (true) {
            String message = "A ping request message was received from Peer " + peer + ".";
            nb_send += 1;
            byte[] buf = message.getBytes();
            DatagramPacket req = new DatagramPacket(buf, buf.length, InetAddress.getLocalHost(), 50000+succ_1);
            socket.send(req);
            req = new DatagramPacket(buf, buf.length, InetAddress.getLocalHost(), 50000+succ_2);
            socket.send(req);
            Thread.sleep(GAP);
        }
    }

}
