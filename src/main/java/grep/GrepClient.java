package grep;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

/**
 * this is the entrance of the program
 * we will get the ArgsToServer inpput here and use it to start the grepClinentThread
 */
public class GrepClient {

    private String configFile;

    public GrepClient(String configFile) {
        this.configFile = configFile;
    }

    public static void grep(String[] args) {

        try {
           new GrepClient("config.properties").start(args);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * client start function
     * @param args from user input
     * @throws IOException
     */
    public void start(String[] args) throws IOException {

        //we assume that the position of the config files which records the information of servers is fixed
        Properties pr = new Properties();

        //FileInputStream  inpro = new FileInputStream("./config.properties");

        pr.load(GrepClient.class.getClassLoader().getResourceAsStream(configFile));


        String[] serverAddresses = pr.getProperty("serverAddress").split(",");
        String[] serverPorts = pr.getProperty("serverPorts").split(",");
        String[] fileAddress = pr.getProperty("fileAddress").split(",");

        //we assume that we provide the serverports and fileaddress for each servers
        ArrayList<ServerProperties> servers = new ArrayList<ServerProperties>();

        for (int i = 0; i < serverAddresses.length; i++) {
            servers.add(new ServerProperties(serverAddresses[i], serverPorts[i], fileAddress[i]));
        }

        //deal with the command: grep  -options  regexValues
        if (args.length < 2) {
            System.out.println("You need to input the command: grep [-option] fileaddress");
        }

        StringBuilder  command = new StringBuilder();
        for (int i = 0; i < args.length; i++) {
            command.append(args[i] + " ");
        }

        doGrep(servers, command);

    }

    /**
     * start the client thread to do distributed grep
     * @param servers basic information of server
     * @param command grep command
     */
    private void doGrep(ArrayList<ServerProperties> servers, StringBuilder command) {
        ArrayList<GrepClientThread>  clientThreads = new ArrayList<GrepClientThread>(servers.size());


        for (int i = 0; i < servers.size(); i++) {
            ArgsToServer argsToServer = new ArgsToServer(command.toString(), servers.get(i).getFileAddress());
            GrepClientThread grepThread = new GrepClientThread(argsToServer, servers.get(i));
            grepThread.start();
            clientThreads.add(grepThread);
        }
        /*
         * ensure the main thread wait for all the query thread terminates
         */
        for (GrepClientThread gct : clientThreads) {
            try {
                gct.join();
            } catch (InterruptedException e) {
                System.err.println("The main thread is interrupted.");
            }
        }
    }


}
