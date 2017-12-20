package sdfs;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.io.IOException;
import java.net.Socket;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.ObjectInputStream;
import java.io.DataOutputStream;

/**
 *  This class is used to send the Ip reuqest  message to master
 */
public class RequestIp {
    public static Logger logger = Logger.getLogger(FileOperation.class);
    public static String[] sendMessage;

    public static int fileTransferPort = SDFSMain.socketPort;

    //LeaderElection leader;

    //String leaderIp = leader.getLeaderIp();
    //int leaderPort =  leader.getLeaderPort();
    //RequestIp fst = new RequestIp();

    /**
     * query ips from mater
     * @param message
     * @return queried ips
     */
    public ArrayList<String> queryForIps(String[] message){
        //TODO
        Socket socket;
        boolean done;

        ArrayList<String> returnIps = new ArrayList<String>();
        try {
            String leaderIp = new LeaderElection().getLeaderIp();
            socket = new Socket(leaderIp,fileTransferPort);
            InputStream inputs = socket.getInputStream();
            OutputStream outputs = socket.getOutputStream();
            //Sending message to the server
            DataOutputStream dataos = new DataOutputStream(outputs);

            String UTF = "query_";

            for (int i = 0; i < message.length; i++) {
                UTF += message[i];
                if (i != message.length - 1) {
                    UTF += "_";
                }
            }

            dataos.writeUTF(UTF);
            dataos.flush();

            ObjectInputStream objects = new ObjectInputStream(inputs);

                try {
                    Object readObject = objects.readObject();
                    returnIps = (ArrayList<String>)readObject;
                }
                catch (ClassNotFoundException e)
                {
                    logger.info("..........");
                }
                socket.close();
        } catch (IOException e) {
            logger.info(e);
        }
        return returnIps;
    }
}
