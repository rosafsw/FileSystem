package grep;

import java.io.*;
import java.net.Socket;

/**
 * client thread
 * take charge of communicating with server: send commands and get query lines.
 */
public class GrepClientThread extends Thread {

    private ArgsToServer argsToServer;
    private ServerProperties servers;

    /**
     * constructor
     * @param argsToServer  grep command and file address
     * @param servers  baisc server information for socket
     */
    public GrepClientThread(ArgsToServer argsToServer, ServerProperties servers) {
        this.argsToServer = argsToServer;
        this.servers = servers;
    }

    public void run() {

        //set uo connection with the server
        Socket socket;
        try {
            socket = new Socket(servers.getServerAddress(),Integer.parseInt(servers.getServerPort()));
        } catch (IOException e) {
           // e.printStackTrace();
            System.err.println("Cannot connect to the server: " + servers.getServerAddress()
                                                 + "  at port:" + servers.getServerPort());
            return;
        }

        ObjectOutputStream toServer = null;
        try {
            toServer = new ObjectOutputStream(socket.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }


        BufferedReader toClient = null;
        try {
            toClient = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {

            toServer.writeObject(argsToServer);

        } catch (IOException e) {
            e.printStackTrace();
        }

        //get and print query results
        String line;
        int count = 0;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream pr = new PrintStream(out);
        try {
            while((line = toClient.readLine()) != null) {
                System.out.println("<" + servers.getServerAddress() + "> :" + line);
                count++;
                pr.println(line);
            }
            pr.flush();
            System.out.println(servers.getServerAddress()+ "  query count:  " + count);
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //saveLocalFile(out);
    }


    /**
     * save the query outcomes in local files
     * @param out
     */
    private void saveLocalFile(ByteArrayOutputStream out) {
        //sava in local files
        FileWriter fw;
        File file = new File("/home/shaowen2/testdata/" + "vm" +servers.getServerAddress().substring(15, 17) +"-ouput.log");

        try {
            if (!file.exists()) {
                file.createNewFile();
            }
            fw = new FileWriter(file);
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(out.toString());
            //bw.write("\n" + "Query Count:" + count + "|| Time:" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + "\n");
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
