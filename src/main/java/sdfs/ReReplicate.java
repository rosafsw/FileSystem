package sdfs;

import membership.MemberGroup;
import membership.MemberInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * this class is responsible for replicate files after failure.
 */
public class ReReplicate extends Thread{

    public void run() {

        //first, according to the failure (if any), we reorganize the filelist
        ConcurrentHashMap<String, MemberInfo> membershipList = MemberGroup.membershipList;
        ConcurrentHashMap<String, FileInfo> leaderFileList = SDFSMain.leaderFileList;

        HashSet<String> failedSever = new HashSet<String>();
        ArrayList<String> aliveSevers = new ArrayList<String>();

        boolean filelistChanged = false;
        String debugType = "";

        //find out all the failed servers and alive servers
        for (Map.Entry<String, MemberInfo> entry : membershipList.entrySet()) {
            if (!entry.getValue().getIsActive()) {
                failedSever.add(entry.getValue().getIp());
            } else {
                aliveSevers.add(entry.getValue().getIp());
            }
        }

        //remove failedSevers from fileList
        for (Map.Entry<String, FileInfo> entry : leaderFileList.entrySet()) {
            HashSet<String> ips = entry.getValue().getIps();
            Map<String, Long> relation = entry.getValue().getUpdateTimeRelation();
            ArrayList<String> changes = new ArrayList<String>();

            for (String str : ips) {
                if (failedSever.contains(str)) {
                    filelistChanged = true;
                    changes.add(str);
                    debugType += "1";
                }
            }

            for (int i = 0; i < changes.size(); i++) {
                ips.remove(changes.get(i));
                relation.remove(changes.get(i));
            }
        }

        //arrange the replication tasks for other nodes
        Collections.sort(aliveSevers);
        for (Map.Entry<String, FileInfo> entry : leaderFileList.entrySet()) {
            ArrayList<String> ips = new ArrayList<String>(entry.getValue().getIps());
            if (ips.size() == aliveSevers.size()) {
                continue;
            }

            if (ips.size() == 1 && aliveSevers.size() > 2) {
                int index = aliveSevers.indexOf(ips.get(0));
                String targetServer1 = aliveSevers.get((index + 1) % aliveSevers.size());
                String targetServer2 = aliveSevers.get((index + 1) % aliveSevers.size());

                String[] sendMessage1 = new String[3];
                sendMessage1[0] = "replicate";
                sendMessage1[1] = targetServer1;
                sendMessage1[2] = entry.getKey();

                FileClientThread ftc1 = new FileClientThread(ips.get(0), sendMessage1);
                ftc1.start();

                String[] sendMessage2 = new String[3];
                sendMessage2[0] = "replicate";
                sendMessage2[1] = targetServer2;
                sendMessage2[2] = entry.getKey();

                FileClientThread ftc2 = new FileClientThread(ips.get(0), sendMessage2);
                ftc2.start();

                //here, we update the filelist
                entry.getValue().getIps().add(targetServer1);
                entry.getValue().getIps().add(targetServer2);

                Long currentime = System.currentTimeMillis();
                entry.getValue().getUpdateTimeRelation().put(targetServer1, currentime);
                entry.getValue().getUpdateTimeRelation().put(targetServer2, currentime);

                filelistChanged = true;
                debugType += "2";
            } else if (ips.size() == 2 || ips.size() == 1) {

                int index = aliveSevers.indexOf(ips.get(0));
                String targetServer = ips.get(0);

                while (ips.contains(targetServer)) {
                    targetServer = aliveSevers.get((++index) % aliveSevers.size());
                }

                String[] sendMessage = new String[3];
                sendMessage[0] = "replicate";
                sendMessage[1] = targetServer;
                sendMessage[2] = entry.getKey();

                FileClientThread ftc = new FileClientThread(ips.get(0), sendMessage);
                ftc.start();

                entry.getValue().getIps().add(targetServer);
                entry.getValue().getUpdateTimeRelation().put(targetServer, System.currentTimeMillis());

                filelistChanged = true;
                debugType += "3";
            }
        }

        //then, if the filelist changed, we need to share the updated filelist to other potential masters.
        if (filelistChanged) {
            SDFSMain.shareFileList();
            //System.out.println("-------------------------"+ debugType);
        }
    }
}
