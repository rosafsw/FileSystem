package sdfs;

import membership.MemberGroup;
import membership.MemberInfo;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;

/**
 * responsible for leader election algorithm
 */
public class LeaderElection {
    public static Logger logger = Logger.getLogger(LeaderElection.class);
    public static String Leader1 = "172.22.147.96";
    public static String Leader2 = "172.22.147.97";
    public static String Leader3 = "172.22.147.98";

//    private static String Leader1 = "10.194.193.97";
//    private static String Leader2 = "10.195.9.90";
//    private static String Leader3 = "";

    // get the current Leader IP
    public String getLeaderIp(){
        Map<String, MemberInfo> maps = MemberGroup.membershipList;
        HashSet<String> aliveServers = new HashSet<String>();
        //System.out.println("maps size : " + maps.size());

        for (Map.Entry<String, MemberInfo> entry : maps.entrySet()) {

            if (entry.getValue().getIsActive()) {
                aliveServers.add(entry.getValue().getIp());
            }
        }

        String currentLeader;
        //System.out.println("out of loop");
        //System.out.println("alive servers" + aliveServers);

        if (aliveServers.contains(Leader1)) {
            currentLeader = Leader1;
        } else if (aliveServers.contains(Leader2)) {
            currentLeader = Leader2;
        } else if (aliveServers.contains(Leader3)){
            currentLeader = Leader3;
        } else {
            return null;
        }

//        if (SDFSMain.curLeader != null && SDFSMain.curLeader != currentLeader) {
//
//        }
        return currentLeader;
    }

    /**
     * get all the living potential leaders
     * @return
     */
    public ArrayList<String> getAliveLeaders() {
        Map<String, MemberInfo> maps = MemberGroup.membershipList;
        ArrayList<String> aliveLeaders = new ArrayList<String>();
        for (Map.Entry<String, MemberInfo> entry : maps.entrySet()) {
            if (entry.getValue().getIsActive()) {
                if (entry.getValue().getIp().equals(Leader1)) {
                    aliveLeaders.add(Leader1);
                } else if (entry.getValue().getIp().equals(Leader2)) {
                    aliveLeaders.add(Leader2);
                } else if (entry.getValue().getIp().equals(Leader3));
            }
        }
        return aliveLeaders;
    }


}
