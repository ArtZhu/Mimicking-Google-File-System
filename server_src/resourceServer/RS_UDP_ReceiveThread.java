package resourceServer;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import Utils.Log;
import Utils.Message;
import Utils.NID;
import Utils.Res;
import Utils.Tools;


public class RS_UDP_ReceiveThread implements Runnable{
	private DatagramSocket socket;
	private RS rs;
	
	private Random random = new Random();
	
	public RS_UDP_ReceiveThread(DatagramSocket _socket, RS _rs){
		socket = _socket;
		rs = _rs;
	}

	@Override
	public void run() {
		DatagramPacket receive_packet;
		Message receive;
		String content, request, info;

		while(true){
			receive_packet = Tools.receive(socket);
			Log.pw("ResourceServer", "ResourceServer Received Something ON UDP");


			receive = (Message) Tools.convertToObject(receive_packet.getData());

			content = (String) receive.get("content");

			Log.pw("RM", "Message content: " + content);

			int i = content.indexOf(Res.separator);
			request = content.substring(0, i);
			info = content.substring(i+1);

			switch(request){
			case Res.RM_RS_open_TCP_request_string:
				NID client_nid = (NID) receive.get("client_nid");
				
				ServerSocket server_socket = null;
				int port;
				while(server_socket == null){
					port = random.nextInt(4000) + 5000;
					try {
						server_socket = new ServerSocket(port);
					} catch (IOException e) {
						continue;
					}		
				}
				 
				int server_socket_port = server_socket.getLocalPort();

					Log.pw("ResourceServer", String.format("server_socket at port %d", 
						server_socket_port));

				Thread t = new Thread(new RS_TCP_FileConnectionThread(server_socket, rs));
				t.setDaemon(true);
				t.start();	

				Message to_client_message = null;
				try {
					to_client_message = new Message().
							put("content", Res.RS_client_TCP_string + Res.separator).
							put("TCP_addr", InetAddress.getLocalHost()).
							put("TCP_port", server_socket_port);
				} catch (UnknownHostException e) {
					e.printStackTrace();
				}

					Log.pw("ResourceServer", "Started TCPFileConenctionThread");
				Tools.send(socket, to_client_message,
						client_nid.addr, client_nid.port);

					Log.pw("ResourceServer", "Sent TCP File Connection Info to " + client_nid);

				break;		
			case Res.request_file_info:
				receive.put("file_updates", rs.last_updates());
				
				rs.sendUDP(receive, rs.RM.addr, rs.RM.port);
				break;
				
			case Res.request_file_updates:
				rs.replicaAccordingTo(
						(HashMap<String, Timestamp>) receive.get("file_updates"));
				break;
			}
		}
	}

}
