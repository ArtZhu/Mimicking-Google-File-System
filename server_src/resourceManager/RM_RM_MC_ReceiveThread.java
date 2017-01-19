package resourceManager;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import Utils.Log;
import Utils.Message;
import Utils.NID;
import Utils.Res;
import Utils.Tools;


public class RM_RM_MC_ReceiveThread implements Runnable{
	private MulticastSocket ferm_mc_socket;
	private RM rm;
	
	public RM_RM_MC_ReceiveThread(MulticastSocket _ferm_mc_socket, RM _rm){
		ferm_mc_socket = _ferm_mc_socket;
		rm = _rm;
	}

	private boolean ignoreAllUntilNow = false;
	public synchronized void setIgnoreAllUntilNow( boolean b ){ ignoreAllUntilNow = b; now = new Timestamp(Calendar.getInstance().getTimeInMillis());}
	private Timestamp now;
	public synchronized boolean getIgnoreAllUntilNow() { return ignoreAllUntilNow; }
	@Override
	public void run() {
		Log.pw("RM", "RM_RM_MC_ReceiveThread started");

		DatagramPacket receive_packet;
		Message receive;
		String content, request, info;

		while(true){
			//Log.pw("RM_MC", "Trying to receive something from FERM");
			receive_packet = Tools.receive(ferm_mc_socket);
			//Log.pw("ResourceServer", "RS_MC Received Something");

			receive = (Message) Tools.convertToObject(receive_packet.getData());
			//Log.pw("RM_MC", String.format("received something %s from FERM", receive));

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
				//Log.pw("RM_MC", String.format("broadcasting %s to RMRS group", receive));
				rm.broadcastToRMRS(receive);
				break;
			case Res.update_rm_list_string:
				Log.pw("RM_MC", "RM received update Request");
				TreeMap<Timestamp, NID> new_list = (TreeMap<Timestamp, NID>) receive.get("rm_list");
				
				
				if(getIgnoreAllUntilNow() && now.after(receive.timestamp))
					continue;
				if(!new_list.values().contains(rm.getLocalNID())){
					//I died;
					Log.print("I died");
					
					setIgnoreAllUntilNow(true);
					rm.restart();
				}else{
					rm.setRMUDPs(new_list);
				}
				break;	
				/*
			case Res.pause_heartbeat:
				rm.pauseHeartbeat();
				break;
				*/
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
