package grep;

/**
 * the class records the basic info of each server
 */
public class ServerProperties {

    private String serverAddress;
    private String serverPort;
    private String fileAddress;

    public String getServerAddress() {
        return serverAddress;
    }

    public String getServerPort() {
        return serverPort;
    }

    public String getFileAddress() {
        return fileAddress;
    }

    public ServerProperties(String serverAddress, String serverPort, String fileAddress) {

        this.serverAddress = serverAddress;
        this.serverPort = serverPort;
        this.fileAddress = fileAddress;
    }


}
