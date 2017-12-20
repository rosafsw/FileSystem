package membership;



import org.apache.log4j.Logger;
import sdfs.LeaderElection;
import sdfs.ReReplicate;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/*** This class is used to receive the message from other nodes.
 *
 */

public class ReceiveThread extends Thread{

    public static Logger logger = Logger.getLogger(ReceiveThread.class);
    public static int port = MemberGroup.receivePort;
    public String IP;

    public void run(){

        while (true) {
            byte[] receiveBuffer = new byte[2048];
            try {
                DatagramSocket receiveSocket = new DatagramSocket(port);
                DatagramPacket receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
                receiveSocket.receive(receivePacket);
                //logger.info("Receive message from : " + receivePacket.getAddress());

                byte[] data = receivePacket.getData();
                ByteArrayInputStream bytestream = new ByteArrayInputStream(data);
                ObjectInputStream objInpStream = new ObjectInputStream(bytestream);
                Message message = (Message) objInpStream.readObject();

                IP = receivePacket.getAddress().toString();


                //the following operations should be completed in another thread
                if (MemberGroup.receiveFlag) {
                    MinorOperation operation = new MinorOperation(message, receivePacket.getAddress().toString());
                    operation.start();
                }

                receiveSocket.close();

            } catch (SocketException e) {
                e.printStackTrace();
                logger.error(e);
            } catch (IOException e) {
                e.printStackTrace();
                logger.error(e);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                logger.error(e);
            }
        }
    }


    /**
     * private inner class to deal with minoroperation
     */
    private class MinorOperation extends Thread{
        Message message;
        String sourceIP;
        String[] introducers;

        public MinorOperation(Message message, String sourceIP) {
            this.message = message;
            this.sourceIP = sourceIP;
        }

        public void run() {
            introducers = new Introducers().getIntroducers();

            if (message.type.equals("join")) {
                logger.info("Receive join request from : " + message.content);
                receiveJoin((String)message.content);
            } else if (message.type.equals("leave")) {
                logger.info("Receive leave request form :" + message.content);
                receiveDisseminate((String)message.content);
            } else if (message.type.equals("heartbeat")) {
                logger.info("Receive heartbeat from : " + IP);
                receiveHeartbeat((String)message.content);
            } else if (message.type.equals("disseminate")) {
                logger.info("Receive failure dissemination of" + (String)message.content + " from " + sourceIP);
                receiveDisseminate((String)message.content);
            } else if (message.type.equals("updatelist")) {
                logger.info("Receive list from" + sourceIP);
                receiveUpdatelist(message);
            }
        }

        /**
         * deal with join request
         * two situations: primary introducer receive join request from normal node
         * or potential introducer receive rejoin request from primary introducer.
         * @param ID
         */
        private void receiveJoin(String ID) {
            String[] ids = ID.split(" ");
            String ip = ids[0];
            if (ip.equals(introducers[0])) {
                //rejoin request from primary introducer
                //disseminate the info and transform the list to primary introducer
                logger.info("Receive rejoin request from primary introducer");
                String preId = "";
                for (Map.Entry<String, MemberInfo> entry : MemberGroup.membershipList.entrySet()) {
                    if (entry.getValue().getIp().equals(introducers[0])) {
                        preId = entry.getKey();
                        continue;
                    } else if (entry.getValue().getIsActive() && !entry.getValue().getIp().equals(MemberGroup.machineIp)){
                        logger.info("Inform the node " + entry.getValue().getIp() + "about the rejoin of primary introducer");
                        MemberGroup.singleRequest(entry.getValue().getIp(), "heartbeat", ID);
                    }
                }

                logger.info("Update the entry of the primary introducer");
                MemberGroup.membershipList.remove(preId);
                MemberGroup.membershipList.put(ID, new MemberInfo(introducers[0], System.currentTimeMillis(), true));

                logger.info("transport the membership list to primary introducer");
                sendMemberList(ip);

            } else {
                //join request from normal node to primary introducer
                logger.info("Receive join request from normal node");
                //if it is the rejoin of the ndde, we only need to update the state of the node
                boolean rejoinOrNOt = false;
                String oldId = "";
                for (Map.Entry<String, MemberInfo> entry : MemberGroup.membershipList.entrySet()) {
                    if (entry.getValue().getIp().equals(ip)) {
                        oldId = entry.getKey();
                        rejoinOrNOt = true;
                        break;
                    }
                }

                if (rejoinOrNOt) {
                    //rejoin of node
                    //send update to the other nodes
                    for (Map.Entry<String, MemberInfo> entry : MemberGroup.membershipList.entrySet()) {
                        if (entry.getValue().getIsActive()) {
                            logger.info("Inform the node " + entry.getValue().getIp() + "about the rejoin of node: " + ID);
                            MemberGroup.singleRequest(entry.getValue().getIp(), "heartbeat", ID);
                        }
                    }

                    //update the local list
                    MemberGroup.membershipList.remove(oldId);
                    MemberGroup.membershipList.put(ID, new MemberInfo(ip, System.currentTimeMillis(), true));

                } else {
                    //join of normal node
                    for (Map.Entry<String, MemberInfo> entry : MemberGroup.membershipList.entrySet()) {
                        if (entry.getValue().getIsActive()) {
                            logger.info("Inform the node " + entry.getValue().getIp() + "about the join of node: " + ID);
                            MemberGroup.singleRequest(entry.getValue().getIp(), "heartbeat", ID);
                        }
                    }
                    logger.info("Udate the local alive state of node :" + ID);
                    MemberGroup.membershipList.put(ID, new MemberInfo(ip, System.currentTimeMillis(), true));
                }

                //send the membershiplist to the node
                sendMemberList(ip);

            }
        }

        /**
         * send membership list to the new node.
         * @param ip
         */
        private void sendMemberList(String ip) {
            logger.info("Send the membership list to node :" + ip);
            DatagramSocket socket = null;
            try {
                socket = new DatagramSocket();
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);

                HashMap<String, MemberInfo> entrys = new HashMap<String, MemberInfo>();
                for (Map.Entry<String, MemberInfo> entry : MemberGroup.membershipList.entrySet()) {
                    entrys.put(entry.getKey(), entry.getValue());
                }
                Message message = new Message("updatelist", entrys);
                objectOutputStream.writeObject(message);

                byte[] buffer = byteArrayOutputStream.toByteArray();
                int length = buffer.length;
                DatagramPacket datagramPacket = new DatagramPacket(buffer,length);
                datagramPacket.setAddress(InetAddress.getByName(ip));
                datagramPacket.setPort(MemberGroup.receivePort);
                //since udp is connectless, we send more the message more than once to reduce the influence of loss
                int count = 3;
                while(count > 0) {
                    socket.send(datagramPacket);
                    count--;
                }
            } catch (SocketException e) {
                e.printStackTrace();
                logger.error(e);
            } catch (IOException e) {
                e.printStackTrace();
                logger.error(e);
            }
        }


        /**
         * deal with heartbeat
         * @param ID
         */
        private void receiveHeartbeat(String ID) {
            //two types , rejoin of node or normal heartbeat
            String[] ids = ID.split(" ");
            if (!MemberGroup.membershipList.containsKey(ID)){
                //rejoin of the node, or join of the new node
                logger.info("Update the ID of the node: " + ids[0]);
                String oldid = "";
                for (Map.Entry<String, MemberInfo> entry : MemberGroup.membershipList.entrySet()) {
                   if (entry.getValue().getIp().equals(ids[0])) {
                        oldid = entry.getKey();
                        break;
                    }
                }
                if (!oldid.equals("")) {
                    MemberGroup.membershipList.remove(oldid);
                }

                MemberGroup.membershipList.put(ID, new MemberInfo(ids[0], System.currentTimeMillis(), true));
                MemberGroup.membershipList.get(ID).setScannable(true);
            } else {
                //update the timestamp of the node
                MemberGroup.membershipList.get(ID).setLastActiveTime(System.currentTimeMillis());
                MemberGroup.membershipList.get(ID).setIsActive(true);

                MemberGroup.membershipList.get(ID).setScannable(true);
            }
        }

        /**
         * deal with dissemination and leave request
         * @param ID
         */
        private void receiveDisseminate(String ID) {

            //here add some judege to deal with SDFS leader judge.
            String prevLeader = new LeaderElection().getLeaderIp();

            MemberGroup.membershipList.get(ID).setIsActive(false);
            //set the scannable to false
            MemberGroup.membershipList.get(ID).setScannable(false);

            String curLeader = new LeaderElection().getLeaderIp();

            if (!curLeader.equals(prevLeader) && MemberGroup.machineIp.equals(curLeader)) {

                ScheduledExecutorService sendScheduler = Executors.newScheduledThreadPool(2);
                //before send heartbeat, set to detect the failure regularly
                //logger.info("Start the failure detection thread.");
                System.out.println("start replicating! dissemi");
                ReReplicate reReplicate = new ReReplicate();
                sendScheduler.scheduleAtFixedRate(reReplicate, 0, 1000, TimeUnit.MILLISECONDS);
            }
        }

        /**
         * deal with updatelist request
         * @param message
         */
        private void receiveUpdatelist(Message message) {
            HashMap<String, MemberInfo> memberlist = (HashMap<String, MemberInfo>)message.content;
            for (Map.Entry<String, MemberInfo> entry : memberlist.entrySet()) {
                if (!MemberGroup.membershipList.contains(entry.getKey())) {
                    MemberGroup.membershipList.put(entry.getKey(), new MemberInfo(entry.getValue().getIp(), System.currentTimeMillis(), entry.getValue().getIsActive()));
                }
            }

        }


    }
}
