package sdfs;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/***
 * This class is used to store information of files in the distributed file system
 */

public class FileInfo implements Serializable{

    private String sdfsFileName;
    //private String localFileName;
    private HashSet<String> storeIPS; //servers that store the file
    private HashMap<String, Long> updateTimeRelation; //recode the store server and last update time

    public FileInfo(String sdfsFileName, HashSet<String> ips, HashMap<String, Long> lastUpdateTime)
    {
       this.storeIPS = ips;
       this.updateTimeRelation = lastUpdateTime;
       this.sdfsFileName = sdfsFileName;
    }


    /**
     * return the iplist
     * @return
     */
    public HashSet<String> getIps() {
        return storeIPS;
    }

    //public String getLocalFileName(){ return localFileName; }
    public String getSdfsFileName(){ return sdfsFileName; }

    /**
     * get the last update time
     * @return
     */
    public long getLastUpateTime()
    {
        long lastupdate = 0;
        for (Map.Entry<String, Long> entry : updateTimeRelation.entrySet()) {
            if (entry.getValue() > lastupdate) {
                lastupdate = entry.getValue();
            }
        }
        return lastupdate;
    }

    /**
     * get the last update server
     * @return
     */
    public String getLastUpdateServer() {
        long lastupdate = 0;
        String lastServer = "";
        for (Map.Entry<String, Long> entry : updateTimeRelation.entrySet()) {
            if (entry.getValue() > lastupdate) {
                lastupdate = entry.getValue();
                lastServer = entry.getKey();
            }
        }
        return lastServer;
    }

    public Map<String, Long> getUpdateTimeRelation() {
        return updateTimeRelation;
    }

    public long getLastUpdateTime(String ip) {
        return updateTimeRelation.get(ip);
    }

    public void setLastUpdateTime(String ip, long newtime)
    {
        updateTimeRelation.put(ip, newtime);
    }

}
