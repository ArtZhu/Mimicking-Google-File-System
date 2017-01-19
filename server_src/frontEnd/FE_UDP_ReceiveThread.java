package frontEnd;


import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import Utils.Log;
import Utils.Message;
import Utils.NID;
import Utils.Res;
import Utils.Tools;


public class FE_UDP_ReceiveThread implements Runnable {
	private DatagramSocket socket_udp;
	private FE FE;
	
	private Date date = new Date();
	
	public FE_UDP_ReceiveThread(DatagramSocket _socket_udp, FE _FE){
		socket_udp = _socket_udp;
		FE = _FE;
	}

	@Override
	public void run() {
		DatagramPacket receive_packet;
		Message receive;
		String content, request, info;
		
		NID RM_nid;
		
		while(true){
			receive_packet = Tools.receive(socket_udp);
			
			receive = (Message) Tools.convertToObject(receive_packet.getData());
			content = (String) receive.get("content");
			
			Log.pw("FrontEnd", 
					String.format("Received message_content: %s", content));
			int i = content.indexOf(Res.separator);
			request = content.substring(0, i);
			info = content.substring(i+1);
			
			Timestamp ts = new Timestamp(Calendar.getInstance().getTimeInMillis());
			
			switch(request){
			
			case Res.RM_FE_register_string:
				RM_nid = (NID) receive.get("rm_nid");
					
				FE.registerRM(RM_nid);
				Log.pw("FrontEnd", String.format("RM %s joined at %s\n", RM_nid, ts));
				
				FE.broadcastRMs();
				break;
				
			case Res.file_loc_request_string:
				RM_nid = FE.getLocation(info);
				//RM_addr = InetAddress.getByName(RM_info.substring(0, j));

				content = Res.file_loc_found_string + 
						Res.separator ;

				Tools.send(socket_udp, 
						new Message().
						put("content", content).
						put("dest_addr", receive_packet.getAddress()).
						put("rm_nid", RM_nid)
						,
						receive_packet.getAddress(), receive_packet.getPort());	
				Log.pw("FrontEnd", 
						String.format("replied \"%s\"\n \tto %s:%d\n", 
								content, receive_packet.getAddress(), receive_packet.getPort()));
		
				break;

			case Res.update_rm_list_string:
				
				ArrayList<NID> alives = (ArrayList<NID>) receive.get("alives");
				Log.pw("FE", "FE received alive list: " + alives);
				
				
				FE.updateRMs(alives);
				break;
			default:
				break;
			}		

			Thread.yield();
			
		}
	}
	
	

}

/*
case Res.file_loc_found_string:
	//"file_loc_found" + ":" + "file_name" + ":" +  "TCP_ADDR" + ":" + "TCP_PORT"
	String client_addr_string, client_port_string, remain;
	String TCP_addr_string, TCP_port_string;
	InetAddress client_addr;
	int client_port;
	int k;

	String send;

	//Pattern
	Log.pw("FrontEnd", String.format(
			"FrontEnd Received:%s\n", content));
	Matcher m = Res.addr_port_pattern.matcher(content);
	m.matches(); m.find();
	remain = m.group();
	Log.pw("FrontEnd", String.format(
			"FrontEnd found TCP:%s\n", remain));


	/**
	String response_string = remain.substring((remain.indexOf(Res.separator) + 1));

	k = receive.lastIndexOf(Res.separator);
	client_port_string = receive.substring(k+1);
	client_port = Integer.parseInt(client_port_string);
	remain = receive.substring(0, k);
	k = remain.lastIndexOf(Res.separator);
	client_addr_string = remain.substring(k+1);
	 /**

	k = remain.lastIndexOf(Res.separator);
	TCP_addr_string = remain.substring(0, k);
	TCP_port_string = remain.substring(k+1);

	int forward_part_index = content.indexOf(remain);

	m.find();
	remain = m.group();
	Log.pw("FrontEnd", String.format(
			"FrontEnd found client:%s\n", remain));

	k = remain.lastIndexOf(Res.separator);

	client_addr_string = remain.substring(0, k);
	client_port_string = remain.substring(k+1);

	try {
		client_addr = FE_UDP_Tools.getAddress(remain);
	} catch (UnknownHostException e1) {
		e1.printStackTrace();
	}
	client_port = FE_UDP_Tools.getPort(remain);

	RM_addr = receive_packet.getAddress();
	RM_port = receive_packet.getPort();

	send = String.format("%s" + "%s" + Res.separator + "%s",
			content.substring(0, forward_part_index), TCP_addr_string, TCP_port_string);

	try {
		client_addr = InetAddress.getByName(client_addr_string);
		FE_UDP_Tools.send(socket_udp, send, client_addr, client_port);
		Log.pw("FrontEnd", String.format(
				"sent \"%s\"\n \tto %s\n", send, client_addr_string + Res.separator + client_port_string));
	} catch (UnknownHostException e) {
		// If client dead?
		e.printStackTrace();
	}
	break;
*/
