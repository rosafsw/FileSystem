package membership;

import java.io.Serializable;

/***
 * This class is used to store information of Members in the Membership Group
 */
public class MemberInfo implements Serializable{


    private int location;
    //private String memberId = "";
    private long lastActiveTime;
    private boolean isActive = true;
    private String ip;
    private boolean scannable = false; //only when the scannable is true, monitor will check whether it is alive.




    public MemberInfo( String ip,int location,long lastActiveTime, boolean isActive)
    {
        this.location = location;
        //this.memberId = memberId;
        this.ip = ip;
        this.lastActiveTime = lastActiveTime;
        this.isActive = isActive;
    }

    public MemberInfo(String ip, long lastActiveTime, boolean isActive) {
        this.ip = ip;
        this.lastActiveTime = lastActiveTime;
        this.isActive = isActive;
    }

    // here we define some function to get or set the members' information
    public int getmemberLocation()
    {

        return location;
    }


    public String getIp() {
        return ip;
    }


    public long getActiveTime()
    {
        return lastActiveTime;
    }

    public void setLastActiveTime(long lastActiveTime)
    {
        this.lastActiveTime = lastActiveTime;
    }

    public boolean getIsActive() {
        return isActive;
    }

    public void setIsActive(boolean isActive) {
        this.isActive = isActive;
    }

    public boolean isScannable() {
        return scannable;
    }

    public void setScannable(boolean scannable) {
        this.scannable = scannable;
    }


}
