package membership;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/***
 * This class is used to send heartbeat  to successor and predecessor
 */

public class SendThread extends Thread {

    public static Logger logger = Logger.getLogger(SendThread.class);

    public void run() {

        ConcurrentHashMap<String, MemberInfo> list = MemberGroup.membershipList;
        ArrayList<String> ips = new ArrayList<String>();
        //collect all the alive node
        for (Map.Entry<String, MemberInfo> entry : list.entrySet()) {
            if (entry.getValue().getIsActive()) {
                ips.add(entry.getValue().getIp());
            }
        }

        //sort by ip
        Collections.sort(ips);
        int size = ips.size();
        int index = ips.indexOf(MemberGroup.machineIp);

        if (size <= 5) {
            for (String ip : ips) {
                if (!ip.equals(MemberGroup.machineIp)) {
                    MemberGroup.singleRequest(ip, "heartbeat", MemberGroup.machineId);
                }
            }
        } else {
            //send heartbeat to two successors and two pre-successors
            MemberGroup.singleRequest(ips.get((index + 1) % size), "heartbeat", MemberGroup.machineId);
            MemberGroup.singleRequest(ips.get((index + 2) % size), "heartbeat", MemberGroup.machineId);

            int newIndex = index - 1;
            newIndex = newIndex < 0 ? newIndex + size : newIndex;
            MemberGroup.singleRequest(ips.get(newIndex), "heartbeat", MemberGroup.machineId);
            int newIndex2 = index - 2;
            newIndex2 = newIndex2 < 0 ? newIndex2 + size : newIndex2;

            MemberGroup.singleRequest(ips.get(newIndex2), "heartbeat", MemberGroup.machineId);

            logger.info("Send heartbeat to nodes: " + ips.get((index + 1) % size) + "," + ips.get((index + 2) % size) +
                                                    "," + ips.get(newIndex) + "," + ips.get(newIndex2));
        }
    }



}
