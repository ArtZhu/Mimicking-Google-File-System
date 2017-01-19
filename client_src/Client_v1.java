import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;

import Util.Log;
import Util.Message;
import Util.Res;
import Util.Tools;


public class Client_v1 implements FileSystemClient{
	
	private InetAddress 	fe_addr;
	private int				fe_port;
	
	private DatagramSocket	socket;
	
	public Client_v1() throws IOException{
		File f = new File(client_config_file_name);
		FileReader fis = null;
		try {
			fis = new FileReader(f);
		} catch (FileNotFoundException e) {
			System.out.printf("Client Config File Not Found: %s\n", client_config_file_name);
		}
		BufferedReader br = new BufferedReader(fis);
		
		String line = br.readLine();
		while(line != null){
			if(line.contains(Res.server_string)){
				line = br.readLine();
				int i = line.indexOf(Res.separator);
				
				fe_addr	= InetAddress.getByName(line.substring(0, i));
				fe_port = Integer.parseInt(line.substring(i+1));
			}
			//
			//if(line.contains(""))
			line = br.readLine();
		}

		//Init socket
		socket = new DatagramSocket();
	}
	
	
	/****
	 * Download
	 */
	public synchronized void downloadFile(String file_name){
		InetAddress TCP_addr = null;
		int 		TCP_port = -1;
		//Request Server Addr and port
		//String request_string = Res.upload_string + Res.separator + file_name;
		Message response = requestFileLocation(file_name);
		Log.pw("Client_v1", String.format("file %s at %s\n", file_name, response));
		
		TCP_addr = (InetAddress) response.get("TCP_addr");
		TCP_port = (Integer) response.get("TCP_port");
		
		Thread t = new Thread(new TCPFileDownloadThread(TCP_addr, TCP_port, file_name));
		t.start();
		
		try {
			t.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/****
	 * Upload
	 */
	@Override
	public synchronized void uploadFile(String file_name) {
		InetAddress TCP_addr = null;
		int 		TCP_port = -1;

		Message response = requestFileLocation(file_name);
		Log.pw("Client_v1", String.format("file %s at %s\n", file_name, response));

		TCP_addr = (InetAddress) response.get("TCP_addr");
		TCP_port = (Integer) response.get("TCP_port");

		Thread t = new Thread(new TCPFileUploadThread(TCP_addr, TCP_port, file_name));
		t.start();		
		
		try {
			t.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private Message requestFileLocation(String file_name){
		Message to_fe_message = new Message().
								put("content", Res.file_loc_request_string + Res.separator + file_name);
		Tools.send(socket, to_fe_message, 
				fe_addr, fe_port);
		return (Message) Tools.convertToObject(
				Tools.receive(socket).getData());
	}
		
}
