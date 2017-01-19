package resourceManager;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import Utils.Log;
import Utils.Message;
import Utils.NID;
import Utils.Res;
import Utils.Tools;


public class RM_RS_MC_ReceiveThread implements Runnable{
	private MulticastSocket rmrs_mc_socket;
	private RM rm;
	
	public RM_RS_MC_ReceiveThread(MulticastSocket _rmrs_mc_socket, RM _rm){
		rmrs_mc_socket = _rmrs_mc_socket;
		rm = _rm;
	}

	@Override
	public void run() {
		Log.pw("RM", "RM_RS_MC_ReceiveThread started");

		DatagramPacket receive_packet;
		Message receive;
		String content, request, info;

		while(true){
			//Log.pw("RM_MC", "Trying to receive something from RMRS");
			receive_packet = Tools.receive(rmrs_mc_socket);
			//Log.pw("ResourceServer", "RS_MC Received Something");

			receive = (Message) Tools.convertToObject(receive_packet.getData());
			//Log.pw("RM_MC", String.format("received something %s from RMRS", receive));
			
			content = (String) receive.get("content");

			//Log.pw("RM", "Message content: " + content);

			int i = content.indexOf(Res.separator);
			request = content.substring(0, i);
			info = content.substring(i+1);

			/*	ignore message from self */
			if(rm.getLocalNID().equals((NID) receive.get("NID"))){
				System.out.println("FERM_FROM MYSELF!");
				continue;
			}
			
			switch(request){
			case Res.update_file_string:
				receive.put("NID", rm.getLocalNID());
				//Log.pw("RM_MC", String.format("broadcasting %s to FERM group", receive));
				rm.broadcastToFERM(receive);
				break;
			default:
				break;
			}


			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Thread.yield();
		}
	}
}
