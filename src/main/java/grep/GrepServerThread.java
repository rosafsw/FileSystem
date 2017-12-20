package grep;

import java.io.*;
import java.net.Socket;


/**
*This class represents the action process of the server
*when it recieves a client request.
*/
public class GrepServerThread extends Thread {

	private Socket socket = null;

	public GrepServerThread(Socket socket) {

		this.socket = socket;
	}


	public void run() {

		BufferedReader breader;
		PrintWriter out = null;

		try {

			ArgsToServer commandInfo = (ArgsToServer) new ObjectInputStream(socket.getInputStream()).readObject();
			out = new PrintWriter(socket.getOutputStream(),true);

			//analyse the output from the client
			Process pro = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", commandInfo.getCommand()+" "+commandInfo.getFileAddress()});

			// output the information to the client

			breader = new BufferedReader(new InputStreamReader(pro.getInputStream()));

			String info;

			while((info = breader.readLine()) != null) {
				out.println(info);
			}


			socket.shutdownInput();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}finally {
			try {
				if (out != null) {
					out.close();
				}
				if (socket != null)
					socket.close();
				}catch(IOException e){
					e.printStackTrace();
				}
			}
		}

	}