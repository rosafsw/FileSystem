package sdfs;


import grep.GrepClient;
import grep.GrepServer;
import membership.MemberGroup;
import org.apache.log4j.Logger;

import java.io.*;
import java.lang.reflect.*;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/***
 * This is the entry of the distributed file system, where members choose to
 *   put/get/delete files in the system.
 */

public class SDFSMain {

    public static Logger logger = Logger.getLogger(SDFSMain.class);
    private static String localFileName;
    private static String sdfsFileName;
    public static ConcurrentHashMap<String, FileInfo> leaderFileList = new ConcurrentHashMap<String, FileInfo>();
    public static MemberGroup memberGroup = new MemberGroup();
    public static String SDFSADDRESS = "/home/shaowen2/mp3/sdfs";
    public boolean rejoin = false;

    public static int socketPort = 4444;
    public static String localIP;

    public static String curLeader;

    public static void main(String[] args) {

        SDFSMain sdfsMain = new SDFSMain();
        sdfsMain.start();
        return;
    }

    private void start() {

        try {
            localIP = InetAddress.getLocalHost().getHostAddress().toString();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        curLeader = new LeaderElection().getLeaderIp();

        //start the grep server for grep query
        //new GrepServer("8090").start();

        MemberGroup.logEntry();

        // first join the membership group
        System.out.println(" Enter 'join' to join the system first");

        // ask the member to choose its action
        System.out.println("\nAfter join in the sytem, you can choose the following action: ");
        System.out.println("\nEnter 'membership' to modify the membership group");
        System.out.println("\nEnter 'id' to show the membership id");
        System.out.println("\nEnter 'leave' to leave the system");
        //System.out.println("\nEnter 'grep' and queries to grep\n");
        System.out.println("\nEnter 'put localfilename sdfsfilename' to insert or update the file");
        System.out.println("\nEnter 'get sdfsfilename localfilename' to get the file from the SDFS");
        System.out.println("\nEnter 'delete sdfsfilename' to delete the file from the SDFS");
        System.out.println("\nEnter 'ls sdfsfilename' to list all members storing this file");
        System.out.println("\nEnter 'store sdfsfilename' to list all files storing in this member\n");


        while (true) {
            InputStreamReader is_reader = new InputStreamReader(System.in);
            String memberActionline = "";
            try {
                memberActionline = new BufferedReader(is_reader).readLine();
            } catch (IOException e) {
                logger.error(e);
                e.printStackTrace();
            }
            String[] memberAction = memberActionline.split(" ");

            // act according to member's action
            if (memberAction[0].equalsIgnoreCase("join")) {
                joinSystem();
            } else if (memberAction[0].equalsIgnoreCase("put")) {
                if (memberAction.length != 3) {
                    System.out.println("Wrong command, please input correct command");
                    continue;
                }
                putFile(memberAction[1], memberAction[2]);
            } else if (memberAction[0].equalsIgnoreCase("get")) {
                if (memberAction.length != 3) {
                    System.out.println("Wrong command, please input correct command");
                    continue;
                }
                getFile(memberAction[2], memberAction[1]);
            } else if (memberAction[0].equalsIgnoreCase("delete")) {
                deleteFile(memberAction[1]);
            } else if (memberAction[0].equalsIgnoreCase("ls")) {
                listMember(memberAction[1]);
            } else if (memberAction[0].equalsIgnoreCase("store")) {
                listFiles();
            } else if (memberAction[0].equalsIgnoreCase("membership")) {
                memberGroup.listMembership();
            } else if (memberAction[0].equalsIgnoreCase("id")) {
                memberGroup.listMemberId();
            } else if (memberAction[0].equalsIgnoreCase("leave")) {
                File file = new File(SDFSADDRESS);
                rejoin = true;
                deleteDir(file, 0);
                memberGroup.leaveGroup();
            } else if (memberAction[0].equalsIgnoreCase("grep")) {

                GrepClient.grep(memberAction);

            } else if (memberAction[0].equalsIgnoreCase("length")) {
                if (memberAction.length != 3) {
                    System.out.println("Wrong command, please input correct command");
                    continue;
                }
                fileSize(memberAction[1], memberAction[2]);
            } else if (memberAction[0].equalsIgnoreCase("less")) {
                if (memberAction.length != 3) {
                    System.out.println("Wrong command, please input correct command");
                    continue;
                }
                queryFileUsingLess(memberAction[1], memberAction[2]);
            } else {
                System.out.println("wrong operation!  please input put, get, delete, ls, store, membership or grep command!");
                logger.info("wrong operation!  please input put, get, delete, ls, store, membership or grep command!");
            }
        }
    }


    public void queryFileUsingLess(String position, String filename) {
        if (position.equals("local")) {
            String command = "less " + FileClientThread.LOCALADDRESS + filename;
            try {
                Process p = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", command});
                BufferedReader breader = new BufferedReader(new InputStreamReader(p.getInputStream()));

                String info;

                while((info = breader.readLine()) != null) {
                    System.out.println(info);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (position.equals("sdfs")) {
            String command = "less " + SDFSADDRESS + "/" + filename;
            try {
                Process p = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", command});

                BufferedReader breader = new BufferedReader(new InputStreamReader(p.getInputStream()));

                String info;

                while((info = breader.readLine()) != null) {
                    System.out.println(info);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * query the file size
     */
    public void fileSize(String position, String fileName) {
        if (position.equals("local")) {
            File file = new File(FileClientThread.LOCALADDRESS + fileName);
            System.out.println("size of local file " + fileName + " is " + file.length());
        } else if (position.equals("sdfs")) {
            File file = new File(SDFSADDRESS + "/" + fileName);
            System.out.println("size of sdfs file " + fileName + " is " + file.length());
        }
    }

    /**
     * send join request to the introducer
     */
    public void joinSystem() {

        File file = new File(SDFSADDRESS);
        deleteDir(file, 0);
        memberGroup.joinGroup();
        //start the file sever thread if it's not rejoin
        if (!rejoin) {
            FileSeverThread severThread = new FileSeverThread();
            severThread.start();
        }

        if (!rejoin && localIP.equals(LeaderElection.Leader1)) {

            System.out.println("start the re-replicate!");

            ScheduledExecutorService sendScheduler = Executors.newScheduledThreadPool(2);
            //before send heartbeat, set to detect the failure regularly
            //logger.info("Start the failure detection thread.");
            ReReplicate reReplicate = new ReReplicate();
            sendScheduler.scheduleAtFixedRate(reReplicate, 0, 1000, TimeUnit.MILLISECONDS);
        }

    }

    /**
     * @param dir
     * @return
     */
    private static boolean deleteDir(File dir, int depth) {
        if (dir.isDirectory()) {
            String[] children = dir.list();

            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]), depth + 1);
                if (!success) {
                    return false;
                }
            }
        }
        if (depth == 0) {
            return true;
        }
        return dir.delete();
    }

    /**
     * share the file list to other potential masters
     */
    public static void shareFileList() {
        Socket socket;
        boolean done;
        LeaderElection election = new LeaderElection();
        ArrayList<String> leaders = election.getAliveLeaders();

        String localIP = null;
        try {
            localIP = InetAddress.getLocalHost().getHostAddress().toString();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        for (String leader : leaders) {
            if (leader.equals(localIP)) {
                continue;
            }

            try {
                socket = new Socket(leader, SDFSMain.socketPort);

                OutputStream outputs = socket.getOutputStream();
                DataOutputStream dataOps = new DataOutputStream(outputs);
                dataOps.writeUTF("shareList");
                dataOps.flush();

                ObjectOutputStream objects = new ObjectOutputStream(outputs);
                objects.writeObject(SDFSMain.leaderFileList);
                objects.flush();
                objects.close();

                logger.info("Sent file lists to" + leader);
                socket.close();
                return;
            } catch (IOException e) {
                logger.info(e);
            }
        }
    }

    /**
     * send put request to the master
     */
    public void putFile(String localfilename, String sdfsfilename) {
        localFileName = localfilename;
        sdfsFileName = sdfsfilename;
        FileOperation put = new FileOperation();
        put.putFile(localFileName, sdfsFileName);

    }

    /**
     * send get request to the master
     */
    public void getFile(String localfilename, String sdfsfilename) {
        localFileName = localfilename;
        sdfsFileName = sdfsfilename;
        FileOperation get = new FileOperation();
        get.getFile(localFileName, sdfsFileName);
    }

    /**
     * send delete request to the master
     */
    public void deleteFile(String sdfsfilename) {
        sdfsFileName = sdfsfilename;
        FileOperation delete = new FileOperation();
        delete.deleteFile(sdfsFileName);
    }

    /**
     * send listmember request to the master and list the address
     * where this file is currently being stored
     */
    public void listMember(String sdfsfilename) {
        sdfsFileName = sdfsfilename;
        FileOperation list = new FileOperation();
        //TODO
        ArrayList<String> addresses = list.listMembers(sdfsFileName);

        if (addresses.size() > 0) {
            System.out.println("File " + sdfsFileName + "is currently storing at addresses\n" + addresses);
        } else {
            System.out.println("File  " + sdfsfilename + " doesn't exist in the system");
        }
    }

    /**
     * send listFiles request to the master and
     * list the files currently being stored at this machine
     */
    public void listFiles() {
//        String machineIp = MemberGroup.machineIp;
//        FileOperation list = new FileOperation();
//        String[] files = list.listFiles(machineIp);
        File file = new File(SDFSADDRESS);

        String[] files = null;
        if (file.isDirectory()) {
            files = file.list();
        }

        //TODO
        System.out.println("The following files are stored at this machine:\n");
        for (int i = 0; i < files.length; i++) {
            System.out.print("[" + files[i] + "] ");
        }

    }

}
