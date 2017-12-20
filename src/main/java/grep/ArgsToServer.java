package grep;

import java.io.Serializable;

/**
 * we use this class to encapsulate the command and file address
 */
public class ArgsToServer implements Serializable {


    private String command;
    private String fileAddress;

    public ArgsToServer(String command, String fileAddress) {
        this.command = command;
        this.fileAddress = fileAddress;

    }

    public String getCommand() {
        return command;
    }

    public String getFileAddress() {
        return fileAddress;
    }


}
