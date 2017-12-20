package sdfs;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Time;
import java.util.*;


/**
 * class for organizing the file operation
 */
public class FileOperation {
    public static Logger logger = Logger.getLogger(FileOperation.class);
    //public static String[] sendMessage;
    RequestIp rp;
    private boolean outoftime = false;

    /**
     * responsible for the time-time bounded conflict
     */
    private class outOfTime extends TimerTask {
        public void run() {
            System.out.println("Out of time! Please enter anything to exit!");
            outoftime = true;
        }
    }

    // send putfile request to the leader and get the ips for operation
    public void putFile(String localfilename, String sdfsfilename) {
        ArrayList<String> ips;
        String[] sendMessage = new String[3];
        sendMessage[0] = "put";
        sendMessage[1] = localfilename;
        sendMessage[2] = sdfsfilename;

        rp = new RequestIp();
        ips = rp.queryForIps(sendMessage);


        if (ips.get(0).equals("judge")) {
            System.out.println("Are you sure to update the file? [yes/no]");
            long currenttime = System.currentTimeMillis();
            Timer timer = new Timer();
            timer.schedule(new outOfTime(), 30 * 1000);
            outoftime = false;

            while (true) {
                InputStreamReader is_reader = new InputStreamReader(System.in);
                String answer = "";
                try {
                    answer = new BufferedReader(is_reader).readLine();
                } catch (IOException e) {
                    logger.error(e);
                    e.printStackTrace();
                }

                if (outoftime) {
                    return;
                }

                // act accroding to member's action
                if (answer.equalsIgnoreCase("yes")) {
                    timer.cancel();
                    break;
                } else if (answer.equalsIgnoreCase("no")) {
                    timer.cancel();
                    System.out.println("You cancelled the put operation!");
                    return;
                } else {
                    System.out.println("please enter the correct command!");
                }
            }
            ips.remove(0);
        }

        //we need to judge whether ips contains my own ip
        for (String str : ips) {
            FileClientThread ftc1 = new FileClientThread(str, sendMessage);
            ftc1.start();
        }
    }

    // send getfile request to the leader and get the ips for operation
    public void getFile(String localfilename, String sdfsfilename) {
        ArrayList<String> ips;
        String[] sendMessage = new String[3];
        sendMessage[0] = "get";
        sendMessage[1] = localfilename;
        sendMessage[2] = sdfsfilename;

        rp = new RequestIp();
        ips = rp.queryForIps(sendMessage);

        if (ips.size() == 0) {
            System.out.println("Sorry, the file is not available");
        }

        //we need to judge whether ips contains my own ip
        if (ips.size() != 0) {
            for (String str : ips) {
                FileClientThread ftc2 = new FileClientThread(str, sendMessage);
                ftc2.start();
            }
        }
    }

    // send deletefile request to the leader and get the ips for operation
    public void deleteFile(String sdfsfilename) {
        ArrayList<String> ips;
        String[] sendMessage = new String[2];
        sendMessage[0] = "delete";
        sendMessage[1] = sdfsfilename;

        rp = new RequestIp();
        ips = rp.queryForIps(sendMessage);

        if (ips.size() == 0) {
            System.out.println("File " + sdfsfilename + " doesn't exist in the system");
        }

        if (ips.size() != 0) {
            for (String ip : ips) {
                FileClientThread ftc = new FileClientThread(ip, sendMessage);
                ftc.start();
            }
        }
    }

    // query the leader for listing all addresses storing the file and return addresses
    public ArrayList<String> listMembers(String sdfsfilename) {
        ArrayList<String> ips;
        String[] sendMessage = new String[2];
        sendMessage[0] = "listmembers";
        sendMessage[1] = sdfsfilename;
        rp = new RequestIp();
        ips = rp.queryForIps(sendMessage);
        return ips;
    }
}
