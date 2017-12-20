package grep;


import java.io.IOException;
import java.net.ServerSocket;


/**
*This class defines the GrepServer which listens to the clients
*and build thread for each connecting client.
*/
public class GrepServer extends Thread{

	String args;
	public GrepServer(String args){
		this.args = args;
	}
	public void run() {

//		if (args.length != 1) {
//			System.err.println("You need to input an port for the server");
//		}

		int serverPort = Integer.parseInt(args);
		ServerSocket serverSocket = null;

		try {
			serverSocket = new ServerSocket(serverPort);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("Server successfully started at port" + serverPort);
		listenToClient(serverSocket);

	}

	/**
	 * waiting for connection request from clients
	 * @param serverSocket
	 */
	private void listenToClient(ServerSocket serverSocket) {
		while(true) {
			GrepServerThread serverThread = null;
			try {
				System.out.println("Waiting for connection");
				serverThread = new GrepServerThread(serverSocket.accept());
			} catch (IOException e) {
				e.printStackTrace();
			}
			serverThread.start();
			System.out.println("The server successfully connected to the client");
		}
	}
}