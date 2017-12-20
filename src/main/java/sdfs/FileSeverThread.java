package sdfs;

import membership.MemberGroup;
import membership.MemberInfo;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * this class is responsible for dealing with request from other nodes
 */
public class FileSeverThread extends Thread {

    //private Socket socket;
    public static Logger logger = Logger.getLogger(FileOperation.class);
    private String SDFSADDRESS = SDFSMain.SDFSADDRESS + "/";
    private ServerSocket ssocket = null;
    private Socket socket = null;


    public void run() {

        InputStream input;
        OutputStream output;
        DataInputStream inputMessage = null;

        try {
            ssocket = new ServerSocket(4444);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {

            while (true) {
                socket = ssocket.accept();

                input = socket.getInputStream();
                output = socket.getOutputStream();
                inputMessage = new DataInputStream(input);

                String message = inputMessage.readUTF();
                String[] ops = message.split("_");
                String type = ops[0];

                //System.out.println("FileSeverThread ops size :" + ops.length);

                if (type.equalsIgnoreCase("put")) {

                    int bytesRead;
                    String filename = ops[2];

                    File outputfile = new File (SDFSADDRESS + filename);
                    outputfile.createNewFile(); //if exists, do nothing
                    OutputStream out = new FileOutputStream(outputfile);


                    long size = inputMessage.readLong();


                    System.out.println("InputMessage size : " + size);

                    byte[] buffer = new byte[1024];
                    while (size > 0 && (bytesRead = inputMessage.read(buffer, 0, (int) Math.min(buffer.length, size))) != -1) {
                        out.write(buffer, 0, bytesRead);
                        size -= bytesRead;
                    }

                    output.close();
                    inputMessage.close();
                    socket.close();
                    System.out.println("File " + filename + " received from client" + socket.getInetAddress().getHostAddress().toString());


                } else if (type.equalsIgnoreCase("get")) {

                    String filename = ops[2];

                    File myFile = new File(SDFSADDRESS + filename);

                    byte[] mybytearray = new byte[(int) myFile.length()];
                    System.out.println("File read into bytesream");
                    FileInputStream fis = new FileInputStream(myFile);
                    BufferedInputStream bis = new BufferedInputStream(fis);

                    DataInputStream dis = new DataInputStream(bis);

                    System.out.println("dis created");
                    dis.readFully(mybytearray, 0, mybytearray.length);
                    DataOutputStream responseMessage = new DataOutputStream(output);
                    responseMessage.writeLong(mybytearray.length);
                    responseMessage.write(mybytearray, 0, mybytearray.length);
                    responseMessage.flush();
                    System.out.println("File " + filename + " sent to Server.");

                    socket.close();
                } else if (type.equalsIgnoreCase("delete")) {

                    String filename = ops[1];
                    File file = new File(SDFSADDRESS + filename);

                    if (!file.exists()) {
                        System.out.println("file doesn't exist");
                    } else if (file.delete()) {
                        System.out.println("file " + filename + " delete sucessfully");
                        logger.info("file " + filename + " delete sucessfully");
                    }

                } else if (type.equalsIgnoreCase("replicate")) {
                    String[] newmessage = new String[3];
                    newmessage[0] = "put_re";
                    newmessage[1] = "fixed";
                    newmessage[2] = ops[2];

                    FileClientThread ftc = new FileClientThread(ops[1], newmessage);
                    ftc.start();

                } else if (type.equalsIgnoreCase("query")) {

                    ObjectOutputStream objectOutput = new ObjectOutputStream(output);
                    ArrayList<String> ips = leaderListOp(ops);

                    objectOutput.writeObject(ips);
                    objectOutput.flush();
                    objectOutput.close();

                } else if (type.equalsIgnoreCase("shareList")) {
                    ObjectInputStream objectInput = new ObjectInputStream(input);

                    try {
                        Object object = objectInput.readObject();
                        ConcurrentHashMap<String, FileInfo> receivedList = (ConcurrentHashMap<String, FileInfo>) object;
                        SDFSMain.leaderFileList = receivedList;

                        System.out.println("Successfully update the file list");
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * deal with query requests from other nodes to the master
     * @param message
     * @return
     */
    public ArrayList<String> leaderListOp(String[] message) {

        //TODO only return the ips but not edit the file list yet
        ArrayList<String> selectedIps = new ArrayList<String>();
        Random rand = new Random();

        if (message[1].equalsIgnoreCase("put")) {
            selectedIps = DealPutQuery(message[3]);

            //after the put operation, the fileList updated, thus we need to share the list to other masters
            SDFSMain.shareFileList();

        } else if (message[1].equalsIgnoreCase("get")) {
            //check whether the file is in the filelist
            if (checkExist(message[3])) {
                //return the 2 of 3 ips for client to get the file
                int index = rand.nextInt(3);

                selectedIps.add(SDFSMain.leaderFileList.get(message[3]).getLastUpdateServer());

            } else {
                System.out.println("Can't get! The file doesn't exist!");
                logger.info("Can't get! The file doesn't exit!");
            }
            //when the file doesn't exist, do nothing, just return the empty ip list
        } else if (message[1].equalsIgnoreCase("delete")) {
            //check whether the file is in the filelist
            if (checkExist(message[2])) {
                //return 3 ips to delete the file
                HashSet<String> storeIps = SDFSMain.leaderFileList.get(message[2]).getIps();
                for (String str : storeIps) {
                    selectedIps.add(str);
                }

                //delete the record from the filelist
                SDFSMain.leaderFileList.remove(message[2]);

                //then, share the updated file list
                SDFSMain.shareFileList();

            }
        } else if (message[1].equalsIgnoreCase("listmembers")) {
            if (checkExist(message[2])) {
                HashSet<String> storeIps = SDFSMain.leaderFileList.get(message[2]).getIps();
                for (String str : storeIps) {
                    selectedIps.add(str);
                }
            }
        }

        return selectedIps;
    }

    /**
     * deal with put command query
     * @param filename
     * @return
     */
    private ArrayList<String> DealPutQuery(String filename) {
        ArrayList<String> selectedIps = new ArrayList<String>();
        Random rand = new Random();
        //check whether the file is already in the filelist
        if (checkExist(filename)) {

            FileInfo fileInfo = SDFSMain.leaderFileList.get(filename);
            HashSet<String> storeIps = fileInfo.getIps();
            long lastupdatetime = fileInfo.getLastUpateTime();
            long currenttime = System.currentTimeMillis();
            long interval = currenttime - lastupdatetime;
            boolean conflict = false;
            if (interval < 60 * 1000) {
                conflict = true;
            }

            //return 3 random  ips
            if (storeIps.size() < 3) {
                //provide conflict
                if (conflict) {
                    selectedIps.add("judge");
                }
                for (String str : storeIps) {
                    selectedIps.add(str);
                    fileInfo.setLastUpdateTime(str, currenttime);
                }
            } else {
                //provide conflict
                if (conflict) {
                    selectedIps.add("judge");
                }

                String[] temp = new String[3];
                int i = 0;
                for (String str : storeIps) {
                    temp[i++] = str;
                }
                i = rand.nextInt(3);

                selectedIps.add(temp[i]);
                selectedIps.add(temp[(i + 1) % 3]);
                fileInfo.setLastUpdateTime(temp[i], currenttime);
                fileInfo.setLastUpdateTime(temp[(i + 1) % 3], currenttime);
            }
        } else {

            //System.out.println("not exist file " + filename + " query for IPs" );
            //if file doesn't exit in the system
            ConcurrentHashMap<String, MemberInfo> list = MemberGroup.membershipList;
            ArrayList<String> aliveservers = new ArrayList<String>();
            //collect all the alive node
            for (Map.Entry<String, MemberInfo> entry : list.entrySet()) {
                if (entry.getValue().getIsActive()) {
                    aliveservers.add(entry.getValue().getIp());
                }
            }

            Collections.sort(aliveservers);

            int size = aliveservers.size();
            int index = 0;

            index = aliveservers.indexOf(socket.getInetAddress().getHostAddress().toString());

//            System.out.print("current alive servers: ");
//            for (int i = 0; i < aliveservers.size(); i++) {
//                System.out.print(aliveservers.get(i) + " ");
//            }
//            System.out.print("\ncurrent index" + index + "\n");

            HashSet<String> tempset = new HashSet<String>();
            tempset.add(aliveservers.get(index));
            tempset.add(aliveservers.get((index + 1) % size));
            tempset.add(aliveservers.get((index + 2) % size));


            //System.out.println("choosed servers :");
            HashMap<String, Long> map = new HashMap<String, Long>();
            for (String str : tempset) {

                //System.out.print(str + " ");
                selectedIps.add(str);
                map.put(str, System.currentTimeMillis());
            }

            //insert new information into the file system

            FileInfo newinfo = new FileInfo(filename, tempset, map);
            SDFSMain.leaderFileList.put(filename, newinfo);
        }

        return selectedIps;
    }

    /**
     * check whether the file exists in the system or not
     * @param filename
     * @return
     */
    public boolean checkExist(String filename) {
        return SDFSMain.leaderFileList.containsKey(filename);
    }

    /**
     * check whether the current is master
     */
    public boolean isLeader() {
        LeaderElection leader = new LeaderElection();
        String leaderIp = leader.getLeaderIp();
        try {
            String machineIp = InetAddress.getLocalHost().getHostAddress().toString();
            if (machineIp == leaderIp) {
                return true;
            }
        } catch (UnknownHostException e) {
            logger.error(e);
            e.printStackTrace();
        }
        return false;
    }

}
