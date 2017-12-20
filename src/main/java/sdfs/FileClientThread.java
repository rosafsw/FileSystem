package sdfs;

import membership.MemberGroup;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedInputStream;
import java.net.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Random;


/*** This class is used to receive the file message from other nodes.
 * *
 */

public class FileClientThread extends Thread {
    public static Logger logger = Logger.getLogger(FileClientThread.class);
    public static int port = SDFSMain.socketPort;
    public String ip;
    public Random rand;
    public String currentIp;
    public String[] message;
    public static  String LOCALADDRESS = "/home/shaowen2/mp3/local/";

    LeaderElection leader = new LeaderElection();
    String leaderIp = leader.getLeaderIp();

    public FileClientThread(String ip, String[] message) {
        this.ip = ip;
        this.message = message;
        this.port = port;
    }

    public void run() {

        fileOperation(this.message);

    }

    /**
     * client file operation
     * @param receivedmessage
     */
    public void fileOperation(String[] receivedmessage) {
        Socket socket = null;
        try {
            socket = new Socket(this.ip, this.port);
        } catch (IOException e) {
            logger.info(e);
        }


        if (receivedmessage[0].equalsIgnoreCase("put") || receivedmessage[0].equalsIgnoreCase("put_re")) {
            //deal with normal put operation and replicate put operaton
            try {
                InputStream inputs = socket.getInputStream();
                OutputStream outputs = socket.getOutputStream();
                DataOutputStream dataOps = new DataOutputStream(outputs);

                String UTF = "put_";
                for (int i = 1; i < message.length; i++) {
                    UTF += message[i];
                    if (i != message.length - 1) {
                        UTF += "_";
                    }
                }
                dataOps.writeUTF(UTF);

                dataOps.flush();

                File file;
                if (receivedmessage[0].equalsIgnoreCase("put")) {
                    file = new File(LOCALADDRESS + message[1]);
                    System.out.println("get file from" + LOCALADDRESS + message[1]);
                } else {
                    file = new File(SDFSMain.SDFSADDRESS + "/" + message[2]);
                }


                // turn file into byte
                byte[] bytefile = new byte[(int) file.length()];

                FileInputStream fileInput = new FileInputStream(file);
                BufferedInputStream bufferInput = new BufferedInputStream(fileInput);
                DataInputStream dataInput = new DataInputStream(bufferInput);

                dataInput.readFully(bytefile, 0, bytefile.length);
                dataOps.writeLong((long) bytefile.length);
                dataOps.write(bytefile, 0, bytefile.length);
                dataOps.flush();
                logger.info("Sent file :" + this.message[1] + "to" + this.ip);
                socket.close();
                return;
            } catch (IOException e) {
                System.out.println(e);
                logger.info(e);
            }
        } else if (receivedmessage[0].equalsIgnoreCase("get")) {
            //deal with get operation
            try {
                InputStream inputs = socket.getInputStream();
                OutputStream outputs = socket.getOutputStream();
                DataOutputStream dataOps = new DataOutputStream(outputs);

                String UTF = "";
                for (int i = 0; i < message.length; i++) {
                    UTF += message[i];
                    if (i != message.length - 1) {
                        UTF += "_";
                    }
                }
                dataOps.writeUTF(UTF);
                dataOps.flush();

                DataInputStream input = new DataInputStream(inputs);

                File outputfile = new File (LOCALADDRESS + message[1]);
                outputfile.createNewFile(); //if exists, do nothing
                FileOutputStream out = new FileOutputStream(outputfile);

                byte[] buffer = new byte[1024];
                long size = input.readLong();

                System.out.println("get file size : " + size);

                int bytesRead;
                while (size > 0 && (bytesRead = input.read(buffer, 0, (int) Math.min(buffer.length, size))) != -1) {
                    out.write(buffer, 0, bytesRead);
                    size -= bytesRead;
                }

                out.close();
                input.close();
                socket.close();

                logger.info("File :" + this.message[1] + " received ");

            } catch (IOException e) {
                logger.info(e);
            }
        } else if (receivedmessage[0].equalsIgnoreCase("delete")) {
            //deal with delete operation
            try {
                OutputStream outputs = socket.getOutputStream();
                DataOutputStream dataOps = new DataOutputStream(outputs);

                String UTF = "";
                for (int i = 0; i < message.length; i++) {
                    UTF += message[i];
                    if (i != message.length - 1) {
                        UTF += "_";
                    }
                }
                dataOps.writeUTF(UTF);
                dataOps.flush();

            } catch (IOException e) {
                logger.info(e);
            }
        } else if (receivedmessage[0].equalsIgnoreCase("replicate")) {
            //deal with replicate operation
            try {
                OutputStream outputs = socket.getOutputStream();
                DataOutputStream dataOps = new DataOutputStream(outputs);

                String UTF = "";
                for (int i = 0; i < message.length; i++) {
                    UTF += message[i];
                    if (i != message.length - 1) {
                        UTF += "_";
                    }
                }
                dataOps.writeUTF(UTF);
                dataOps.flush();

            } catch (IOException e) {
                logger.info(e);
            }
        }
    }


}
