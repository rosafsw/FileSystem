package membership;


import grep.GrepClient;
import grep.GrepServer;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

import java.io.*;
import java.net.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.Map.Entry;

/***
 * This is the entry of the project, where nodes join into the group and
 *   generate their membership lists.
 */
public class MemberGroup {

    public static Logger logger = Logger.getLogger(MemberGroup.class);

    // set receive port of every potential member
    public final static int receivePort = 8088;
    public static int timeout = 2000;
    //define some variable of machine
    public static String machineIp = "";
    public static String machineId = "";
    public static long currentTime = System.currentTimeMillis();
    //public static String machineTimestamp = String.valueOf(currentTime);

    ReceiveThread receiveThread;
    SendThread sendThread;
    FailureDetect failureDetect;

    public static boolean rejoin = false;

    public static boolean receiveFlag = true;
    public static boolean sendFlag = true;
    public static boolean scanFlag = true;

    // here we use hashmap to store the information of membership list
    public static ConcurrentHashMap<String, MemberInfo> membershipList = new ConcurrentHashMap<String, MemberInfo>();

    public static void main(String[] args) {

        MemberGroup memberGroup = new MemberGroup();

        //start the grep server for grep query
        new GrepServer("8090").start();


        try {
            machineIp = InetAddress.getLocalHost().getHostAddress().toString();
        } catch (UnknownHostException e) {
            logger.error(e);
            e.printStackTrace();
        }
        machineId = machineIp + " " + currentTime;
        // create a file to store log
        logEntry();

        // ask the member to choose its action
            System.out.println("\nYou can choose the following action: ");
            System.out.println("\nEnter 'membership' to list membership list");
            System.out.println("\nEnter 'id' to list your id ");
            System.out.println("\nEnter 'join' to join the group ");
            System.out.println("\nEnter 'leave' to leave the group ");
            System.out.println("\nEnter 'grep' and queries to grep\n");

        while(true) {
            InputStreamReader is_reader = new InputStreamReader(System.in);
            String memberAction = "";
            try {
              memberAction = new BufferedReader(is_reader).readLine();
            } catch (IOException e) {
                logger.error(e);
                e.printStackTrace();
            }

            String[] grepcommand = memberAction.split(" ");

            // act accroding to member's action
            if(memberAction.equalsIgnoreCase("membership"))
            {
                memberGroup.listMembership();
            }
            else if(memberAction.equalsIgnoreCase("id"))
            {
                memberGroup.listMemberId();
            }
            else if(memberAction.equalsIgnoreCase("join"))
            {
                memberGroup.joinGroup();
            }
            else if(memberAction.equalsIgnoreCase("leave"))
            {
                memberGroup.leaveGroup();
            } else if (grepcommand[0].equals("grep"))
            {
                GrepClient.grep(grepcommand);
            }else
            {
                System.out.println("wrong operation!  please input membership, id, join, leave or grep command!");
                logger.info("wrong operation!  please input membership, id, join or leave!");
            }
        }
    }

    /**
     * initialize the logger
     * @return
     */
    public static boolean logEntry() {
        try {
            boolean append = true;
            PatternLayout pattern = new PatternLayout("[%-5p] %d{ISO8601} %c.class %t: %m %n");
            RollingFileAppender fileAppender = new RollingFileAppender(pattern, "mp3_node.log");
            fileAppender.setLayout(pattern);
            fileAppender.setName("LogFile");
            fileAppender.setMaxFileSize("16MB");
            fileAppender.activateOptions();
            Logger.getRootLogger().addAppender(fileAppender);
            logger.info("CS425_MP2 log file has been created!");
            return true;
        } catch (Exception e) {
            logger.error(e);
            return false;
        }

    }

    /**
     * This function is used to check whether the current machine is the introducer.
     * @return boolean
     * @throws IOException
     */
    public static boolean isPrimaryIntroducer() {
        String[] introducers = new Introducers().getIntroducers();
        if (machineIp.equalsIgnoreCase(introducers[0])) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * list membership list
     * @throws IOException
     */
    public void listMembership() {
        String split = "\t";

        if(membershipList.isEmpty()){
            System.out.println("Please join the group first!");
            logger.info("The membershiplist is empty now, please join the group first!");
            return;
        }else
        {
            for(Entry<String, MemberInfo> entry : membershipList.entrySet())
            {
                MemberInfo list = entry.getValue();
                System.out.println("Your membership list is as follows:\n");
                System.out.println("MemberId"+ split + split + split + "LastActiveTime   IsActive");
                System.out.println(entry.getKey() + split + list.getActiveTime() + split + list.getIsActive());
                logger.info("Member "+machineId+" is viewing its membership list.");
                logger.info("The membershiplist is as follows:");
                logger.info(entry.getKey() + split +list.getmemberLocation()+ split +list.getActiveTime()+ split +list.getIsActive());
            }
        }
    }

    /**
     * list member id
     * @throws IOException
     */
    public void listMemberId() {

        System.out.println("Your id is :"+machineId);
        logger.info("The member "+ machineId +"is viewing its id.");
    }

    /**
     * the join operation can main divided into two types,the join of  normal nodes or the join of primary introducer.
     * And the join of primary introducer can be divided into two types. First, whether it is the first node that join
     * the group or it crashed before and it rejoins now. To keep consistent, we will both send join request of primary
     * introducer command to potential introducers.
     * @throws IOException
     */
    public void joinGroup() {

        try {
            machineIp = InetAddress.getLocalHost().getHostAddress().toString();
        } catch (UnknownHostException e) {
            logger.error(e);
            e.printStackTrace();
        }

        receiveFlag = true;
        machineId = machineIp + " " +System.currentTimeMillis();

        //start the receive Thread
        logger.info("Start the listening thread");

        if (!rejoin) {
            receiveThread = new ReceiveThread();
            receiveThread.start();
        }

        logger.info("Add the node into local membership list.");
        membershipList.put(machineId, new MemberInfo(machineIp, currentTime, true));

        // join the current machine into the distributed group membership
        if (isPrimaryIntroducer()) {
            //send join request to potential introducers
            String[] introducers = new Introducers().getIntroducers();
            for (int i = 1; i < introducers.length;  i++) {
                logger.info("Primary introducer send join request to potential introducers: " + introducers[i]);
                singleRequest(introducers[i], "join", machineId);
            }
        } else {
            logger.info("Send the join request to primary introducer.");
            //send UDP package to introducer
            String[] introducers = new Introducers().getIntroducers();
            singleRequest(introducers[0], "join", machineId);
        }

        if (!rejoin) {
            ScheduledExecutorService sendScheduler = Executors.newScheduledThreadPool(2);
            //before send heartbeat, set to detect the failure regularly
            logger.info("Start the failure detection thread.");
            failureDetect = new FailureDetect();
            sendScheduler.scheduleAtFixedRate(failureDetect, 0, 500, TimeUnit.MILLISECONDS);
            //set to send heartbeat regularly
            logger.info("Start the heartbeat sending thread");
            sendThread = new SendThread();
            sendScheduler.scheduleAtFixedRate(sendThread, 0, 500, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * send join request to the introducer
     * @param ip
     */
    public static void singleRequest(String ip, String command, String relatedID) {
        DatagramSocket socket = null;
        //String[] introducers = new Introducers().getIntroducers();
        try {
            socket = new DatagramSocket();
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);

            if (relatedID.equals("")) {
                objectOutputStream.writeObject(new Message(command, ip));
            } else {
                objectOutputStream.writeObject(new Message(command, relatedID));
            }

            byte[] buffer = byteArrayOutputStream.toByteArray();
            int length = buffer.length;
            DatagramPacket datagramPacket = new DatagramPacket(buffer,length);
            datagramPacket.setAddress(InetAddress.getByName(ip));
            datagramPacket.setPort(receivePort);
            //since udp is connectless, we send more the message more than once to reduce the influence of loss
//            int count = 3;
//            while(count > 0) {
                socket.send(datagramPacket);
//                count--;
//            }
        } catch (SocketException e) {
            e.printStackTrace();
            logger.error(e);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e);
        }
    }

    /**
     * leave the group, inform all the other members
     * @throws IOException
     */
    public void leaveGroup() {



        for (Entry<String, MemberInfo> entry : membershipList.entrySet()) {
            MemberInfo member = entry.getValue();
            if (member.getIsActive() && !member.getIp().equals(machineIp)) {
                logger.info("Send the leaving info to :" + member.getIp());
                singleRequest(member.getIp(), "leave", machineId);
            }
        }
        MemberGroup.membershipList.clear();
        MemberGroup.receiveFlag = false;
        MemberGroup.rejoin = true;
    }

}
