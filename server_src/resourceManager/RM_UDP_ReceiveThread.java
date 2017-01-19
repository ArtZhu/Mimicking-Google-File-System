package resourceManager;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.regex.Matcher;
import java.sql.Timestamp;

import Utils.Log;
import Utils.Message;
import Utils.NID;
import Utils.Res;
import Utils.Tools;


public class RM_UDP_ReceiveThread implements Runnable{	
	private RM_UDP_ReceiveThread _this = this;
	
															private boolean hb_print = false;
	
	private DatagramSocket 	socket;
	private RM				rm;
	
	private SafetyChecker		safetyChecker;
	private Timer		safetyCheckerTimer;
	
	private boolean safe = true;
	public synchronized boolean getSafe(){ return safe; }
	public synchronized void setSafe(boolean _safe){ safe = _safe; }
	
	private ArrayList<NID> alives = new ArrayList<NID>();
	private synchronized void putAlive(NID alive)
	{ 
		if(!alives.contains(alive)) 
			alives.add(alive);
	}
	public synchronized ArrayList<NID> getAlive(){ ArrayList<NID> ret = alives; alives = new ArrayList<NID>(); return ret; }
	
	public RM_UDP_ReceiveThread(DatagramSocket _socket, RM _rm){
		socket = _socket;
		rm = _rm;
	}
	
	
	public synchronized void launchSafetyChecker(){
		resetSafetyChecker();
	}
	/*
	public void pauseSafetyChecker(){
		safetyCheckerTimer.cancel();
	}
	*/
	public synchronized void resumeSafetyChecker(){
		safetyChecker.setGo(true);
	}
	
	public void pauseSafetyChecker(){
		safetyChecker.setGo(false);
	}
	
	
	@Override
	public void run() {	
		DatagramPacket receive_packet;
		Message receive;
		String content, request, info;
		
		//safeChecker = new 
		
		while(true){
			receive_packet = Tools.receive(socket);
			if(hb_print)
				Log.pw("RM", "RM received message");

			receive = (Message) Tools.convertToObject(receive_packet.getData());
			if(Calendar.getInstance().getTimeInMillis() - receive.timestamp.getTime() > 10000L)
				continue;
			content = (String) receive.get("content");
			
			if(hb_print)
				Log.pw("RM", "Message content: " + content);
			
			int i = content.indexOf(Res.separator);
			request = content.substring(0, i);
			info = content.substring(i+1);
			
			switch(request){
			case Res.file_loc_request_string:
				
				String file_name = info;
					Log.pw("RM", 
						String.format("RM received a request for %s location", file_name));
				
				NID rs = rm.balancedLocation(file_name);
				
					Log.pw("RM", 
						String.format("RM found file on %s", rs));
				
				NID client_nid = (NID) receive.get("client_nid");
				Message to_rs_message = new Message().
						put("content", Res.RM_RS_open_TCP_request_string + Res.separator).
						put("client_nid", client_nid);
				
				Tools.send(socket, to_rs_message, rs.addr, rs.port);	
					Log.pw("RM", 
						String.format("RM requested server to open TCP on %s for %s", 
								rs, client_nid));
				break;
			
			case Res.RS_RM_join_as_server_request:
				NID rs_udp = (NID) receive.get("rs_udp");
				Log.pw("RM", 
						String.format("RM received a request to join as server from %s", 
								rs_udp));

				if(rm.registerServer(rs_udp)){
					ArrayList<NID> rs_udps = rm.getRSUDPs();
					Log.pw("RM", String.format("rs_udps %s\n", rs_udps));
					
					to_rs_message = new Message().
							put("content", Res.RM_RS_join_as_server_confirm + Res.separator).
							put("rs_udps", rs_udps);
					
					Tools.send(socket, 
							to_rs_message,
							receive_packet.getAddress(),
							receive_packet.getPort());
					Log.pw("RM", 
							String.format("RM confirmed %s", rs_udp));
				}
				else{
					Tools.send(socket, 
								new Message().
											put("content", Res.RM_RS_join_as_server_reject + Res.separator),
							receive_packet.getAddress(),
							receive_packet.getPort());
					Log.write("RM", 
							String.format("RM rejected %s", rs_udp));
				}
				break;
				
			case Res.heartbeat:				
				int index = (int) receive.get("index");
				
				
				if(hb_print)
					Log.pw("RM", "Heartbeat index: " + index);

				/*
				NID from_nid = (NID) receive.get("NID" + (index-1));
				if(!from_nid.equals(rm.getPriorNid())){
					//tell him he's dead
					Message pause_heartbeat_message = new Message()
								.put("content", Res.pause_heartbeat + Res.separator);
					
					rm.broadcastToFERM(pause_heartbeat_message);
					
					Message to_dead_message = new Message().put("content", Res.you_died_string + Res.separator);
					rm.sendUDP(to_dead_message, from_nid.addr, from_nid.port);
					break;
				}
				*/
				
				if(index == rm.getRMGroupSize()){
					// safe;
					setSafe(true);	
					if(hb_print)
						Log.pw("RM", "Heartbeat back");	
				}else{
					if(index >= rm.getRMGroupSize())
						break;
					
					receive
					.put("NID" + index, rm.getLocalNID())
					.put("index", index + 1);

					NID next_nid = rm.getNextNid();

					rm.sendUDP(receive, next_nid.addr, next_nid.port);
					if(hb_print)
						Log.pw("RM", "Pass On to " + next_nid);	
				}
				
				break;

			case Res.ping:
				
				Message response = new Message().put("content", Res.ack + Res.separator).put("NID", rm.getLocalNID());
				
				Tools.send(socket, 
						response, 
						receive_packet.getAddress(),
						receive_packet.getPort());
				
				safetyChecker.setGo(false);
				
				Log.pw("RM", "Respond to ping from "  + receive_packet.getAddress() + ":" + receive_packet.getPort());	
				break;
				
			case Res.ack:
				NID alive = (NID) receive.get("NID");
				
				putAlive(alive);			

				Log.pw("RM", receive_packet.getAddress() + ":" + receive_packet.getPort() + " is Alive");	
				break;
				
			case Res.you_died_string:
				Log.pw("RM", "I was dead");
				
				//dead dealing;
				//rm.restart();
				
				break;
				
			case Res.request_file_info:
				NID next = rm.getNextNid();
				if(receive_packet.getAddress().equals(next.addr) && receive_packet.getPort() ==  next.port){
					rm.broadcastFileInfoAccordingTo(
							(HashMap<String, Timestamp>) receive.get("file_updates"));
				}else{
					NID prior = rm.getPriorNid();
					rm.sendUDP(receive, prior.addr, prior.port);
				}
				break;
			}//switch
			

			try {
				Thread.sleep(200);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Thread.yield();
			
			
		}
	}
	
	
	private void resetSafetyChecker(){
		safetyChecker = new SafetyChecker();
		safetyCheckerTimer = new Timer();
		safetyCheckerTimer.schedule(safetyChecker, 2000L, 4000L);
	}
	
	class SafetyChecker extends TimerTask{
		private boolean go = true;
		public void setGo(boolean _go) { go = _go; }
		
		@Override
		public void run() {
			System.out.println("check!");
			if(go){
				if(!getSafe()){
					//broadcast
					Log.pw("RM_UDP", "not safe!");

					//ping everyone?
					rm.pingAckAll();

					synchronized(_this){
						try {
							Log.pw("RM_UDP", "SafetyChecker waits!");
							_this.wait();
							Log.pw("RM_UDP", "SafetyChecker goes again!");
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

				}else{
					Log.pw("RM_UDP", "safe!");

					setSafe(false);
				}
				
			}else{
				synchronized(_this){
					try {
						Log.pw("RM_UDP", "SafetyChecker waits!");
						_this.wait();
						Log.pw("RM_UDP", "SafetyChecker goes again!");
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}	
		
	}
}