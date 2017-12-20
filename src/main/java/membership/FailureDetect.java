package membership;

import org.apache.log4j.Logger;
import sdfs.LeaderElection;
import sdfs.ReReplicate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FailureDetect extends Thread {
    public static Logger logger = Logger.getLogger(FailureDetect.class);
    public static int failTime = 3000;
    public static long currentTime;
    public static ArrayList<String> lastScannable = new ArrayList<String>();

    public void run() {
        //System.out.println("scanning");
        currentTime = System.currentTimeMillis();

        //scan the entries of those node
        ConcurrentHashMap<String, MemberInfo> list = MemberGroup.membershipList;
        ArrayList<String> ips = new ArrayList<String>();
        //collect all the alive node ips
        for (Map.Entry<String, MemberInfo> entry : list.entrySet()) {
            if (entry.getValue().getIsActive()) {
                ips.add(entry.getValue().getIp());
            }
        }

        //System.out.println("Ips:" + ips.toString());
        //sort by ip
        Collections.sort(ips);
        int size = ips.size();
        //collect all the ips that need to be scaned
        ArrayList<String> scans = new ArrayList<String>();
        int index = ips.indexOf(MemberGroup.machineIp);
        if (size <= 5) {
            for (String ip : ips) {
                if (!ip.equals(MemberGroup.machineIp)) {
                    scans.add(ip);
                }
            }
        } else {
            scans.add(ips.get((index + 1) % size));
            scans.add(ips.get((index + 2) % size));

            int newIndex = index - 1;
            newIndex = newIndex < 0 ? newIndex + size : newIndex;
            scans.add(ips.get(newIndex));
            newIndex = index - 2;
            newIndex = newIndex < 0 ? newIndex + size : newIndex;
            scans.add(ips.get(newIndex));
        }


        logger.info("lastScannable:" + lastScannable.toString());
        logger.info("scans:" + scans.toString());

        //System.out.println("Scans " + scans.toString());
        for (Entry<String, MemberInfo> entry: MemberGroup.membershipList.entrySet()) {
            if (lastScannable.contains(entry.getValue().getIp()) && !scans.contains(entry.getValue().getIp())) {
                entry.getValue().setScannable(false);
            }
            //the node should be alive and scannable.
            if (scans.contains(entry.getValue().getIp()) && entry.getValue().isScannable()) {
                //System.out.println("has,, " + currentTime + "  " + entry.getValue().getActiveTime());
                if (currentTime - entry.getValue().getActiveTime() > failTime) {
                    //System.out.println("System currenttime" + currentTime + " " + entry.getValue().getActiveTime());
                    //set the state of the node false

                    //here add some judege to deal with SDFS leader judge.
                    String prevLeader = new LeaderElection().getLeaderIp();

                    entry.getValue().setIsActive(false);
                    entry.getValue().setScannable(false);

                    String curLeader = new LeaderElection().getLeaderIp();

                    if (!curLeader.equals(prevLeader) && MemberGroup.machineIp.equals(curLeader)) {

                        ScheduledExecutorService sendScheduler = Executors.newScheduledThreadPool(2);
                        //before send heartbeat, set to detect the failure regularly
                        //logger.info("Start the failure detection thread.");
                        System.out.println("start replicating! failure detect");
                        ReReplicate reReplicate = new ReReplicate();
                        sendScheduler.scheduleAtFixedRate(reReplicate, 0, 1000, TimeUnit.MILLISECONDS);
                    }

                    //disseminate the failure after another failTime
                    for (String ip : ips) {
                        if (!ip.equals(MemberGroup.machineIp) && !ip.equals(entry.getValue().getIp())) {
                            logger.info("Send failue disseminate from" + MemberGroup.machineId + "to" + ip + "about" + entry.getValue().getIp());
                            MemberGroup.singleRequest(ip, "disseminate", entry.getKey());
                        }
                    }
                }
            }
        }
        lastScannable = new ArrayList<String>(scans);

    }
}
