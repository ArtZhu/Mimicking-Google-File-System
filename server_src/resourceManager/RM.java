package resourceManager;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;

import Utils.Log;
import Utils.Message;
import Utils.NID;
import Utils.Res;
import Utils.Tools;

public class RM {
	private List<NID> rs_udps;
	
	private NID primary_udp;
	/*	set Primary info*/
	public void 	setPrimaryUDP(NID _primary_udp)	{	primary_udp = _primary_udp;	}
	
	public NID 	getPrimaryUDP()						{	return primary_udp;	}
	
	public synchronized void 	setRSUDPs(List<NID> _rs_udps)			{	rs_udps = _rs_udps;	}
	/*	Retrieve RS's	*/	
	public synchronized ArrayList<NID> getRSUDPs(){
		ArrayList<NID> rs_list = new ArrayList<NID>();
		rs_list.addAll(rs_udps);
		return rs_list;
	}
	
	/*	Retrieve RS's	*/
	private NID local_nid;
	public NID getLocalNID() { return local_nid; }
	private DatagramSocket udp_socket;
	
	private NID rmrs_mc;
	private MulticastSocket rmrs_mc_socket;
	private NID ferm_mc;
	private MulticastSocket ferm_mc_socket;
	private int				TTL = 10;
	
	private NID FE;
	
	/* RM heartbeats */
	//private ArrayList<NID> rm_udps;
	private Timestamp local_timestamp;
	private TreeMap<Timestamp, NID> rm_udps;
	private boolean updateFlag = false;
	private synchronized void setUpdateFlag(boolean f){ updateFlag = f;}
	private synchronized boolean getUpdateFlag(){ return updateFlag; }
	
	public synchronized void setRMUDPs(TreeMap<Timestamp, NID> _rm_udps){
		Log.pw("RM", "new rm_list: \n " + _rm_udps);
		
		for(Map.Entry<Timestamp, NID> entry: _rm_udps.entrySet()){
			if(entry.getValue().equals(local_nid)){
				local_timestamp = entry.getKey();
				break;
			}
		}
		
		resumeHeartbeats();
		
		rm_udps = _rm_udps;
		synchronized(rm_udp_receive_runnable){
			rm_udp_receive_runnable.notifyAll();
		}
		try{
			heartbeat_runnable.setGo(true);
			synchronized(heartbeat_runnable){
				heartbeat_runnable.notifyAll();
			}
		}catch(NullPointerException e){ /* server just started */ }
		
		rm_udp_receive_runnable.resumeSafetyChecker();
		if(getUpdateFlag()){
			setUpdateFlag(false);
			requestFileUpdates();
		}
	}
	
	private RM_UDP_ReceiveThread rm_udp_receive_runnable;
	private HeartbeatThread heartbeat_runnable;
	
	/*
	public synchronized void pauseHeartbeat(){
		heartbeat_runnable.setGo(false);
		rm_udp_receive_runnable.pauseSafetyChecker();
	}
	
	public synchronized void restartHeartbeat(){
		heartbeat_runnable.setGo(true);
		synchronized(heartbeat_runnable){
			heartbeat_runnable.notifyAll();
		}
		rm_udp_receive_runnable.launchSafetyChecker();
	}
	*/
	
	public synchronized int getRMGroupSize(){
		return rm_udps.size();
	}
	
	public synchronized NID getNextNid() {
		try{
			return rm_udps.lowerEntry(local_timestamp).getValue();
		}catch(NullPointerException e){
			// no lower entry
			return rm_udps.lastEntry().getValue();
		}
	}
	
	public synchronized NID getPriorNid() {
		try{
			return rm_udps.higherEntry(local_timestamp).getValue();
		}catch(NullPointerException e){
			// no higher entry
			return rm_udps.firstEntry().getValue();
		}
	}
	
	
	public void pauseHeartbeats(){
		heartbeat_runnable.setGo(false);
		rm_udp_receive_runnable.pauseSafetyChecker();
	}
	
	public void resumeHeartbeats(){
		heartbeat_runnable.setGo(true);
		rm_udp_receive_runnable.resumeSafetyChecker();
	}
	
	public void pingAckAll(){
		pauseHeartbeats();
		new Thread(new PingAckAllChecker()).start();
	}
	
	/* restart */
	public void restart(){
		Log.pw("RM", "Restarting RM");
		pingAckAll();
		
		//
		setUpdateFlag(true);
	}
	
	/* load balancing */
	private LoadBalancer lb;
	public NID balancedLocation(String file_name){
		return lb.balancedLocation(file_name);
	}
	
	public RM(){
		new RMConfigurer().configure();

		//Init socket
		try {
			udp_socket = new DatagramSocket(local_nid.port);
			rmrs_mc_socket = new MulticastSocket(rmrs_mc.port);
			ferm_mc_socket = new MulticastSocket(ferm_mc.port);
			joinGroup();
		} catch (SocketException e) {} catch (IOException e) {
			e.printStackTrace();
		}
	
	}
	
	private ArrayList<Thread> thread_controls = new ArrayList<Thread>();	
	public void start(){		
		contactFE();
		
		Thread t1 = new Thread(new RM_RM_MC_ReceiveThread(ferm_mc_socket, this));
		t1.setDaemon(true);
		t1.start();
		
		Thread t2 = new Thread(new RM_RS_MC_ReceiveThread(rmrs_mc_socket, this));
		t2.setDaemon(true);
		t2.start();
		
		rm_udp_receive_runnable = new RM_UDP_ReceiveThread(udp_socket, this);
		Thread receive = new Thread(rm_udp_receive_runnable);
		receive.setDaemon(true);
		receive.start();
		
	
		heartbeat_runnable = new HeartbeatThread();
		Thread hb = new Thread(heartbeat_runnable);
		hb.setDaemon(true);
		hb.start();
		rm_udp_receive_runnable.launchSafetyChecker();
		
		thread_controls.addAll(Arrays.asList(t1, t2, receive, hb));
		
		if(rm_udps.size() > 1){
			Log.pw("RM", "Requesting File Info from primary RS");
			requestFileUpdates();
		}
		
		try {
			receive.join();
		} catch (InterruptedException e) {}
	}
	
	public void requestFileUpdates(){
		if(rs_udps.size() > 0){
			NID rs = rs_udps.get(0);
			Message to_rs_message = new Message()
							.put("content", Res.request_file_info + Res.separator);

			sendUDP(to_rs_message, rs.addr, rs.port);
		}
	}

	public synchronized boolean registerServer(NID rs_udp){
		if(rs_udps.size() > 3)
			return false;
		rs_udps.add(rs_udp);
		return true;
	}
	
	public synchronized void removeServer(NID rs_udp){
		rs_udps.remove(rs_udp);
		
		//broadcast implement later
	}
	
	private Random random = new Random();
	public NID getLocation(String file_name){	
		return rs_udps.get(random.nextInt(rs_udps.size()));
	}
	
	/* 	broadcast	*/
	public void broadcastToFERM(Serializable s){
		Tools.send(ferm_mc_socket, s, ferm_mc.addr, ferm_mc.port);
	}
	public void broadcastToRMRS(Serializable s){
		Tools.send(rmrs_mc_socket, s, rmrs_mc.addr, rmrs_mc.port);
	}
	
	public void broadcastFileInfoAccordingTo(HashMap<String, Timestamp> file_updates){
		NID rs = rs_udps.get(0);
		Message to_rs_message = new Message()
				.put("content", Res.request_file_updates + Res.separator)
				.put("file_updates", file_updates);
		sendUDP(to_rs_message, rs.addr, rs.port);
	}
	
	/* send udp */
	public void sendUDP(Serializable s, InetAddress addr, int port){
		Tools.send(udp_socket, s, addr, port);
	}
	
	/*	configure	*/
	private String RM_config_file_name = "RMConfig";
	class RMConfigurer{
		public RMConfigurer(){}

		public void configure(){
			rs_udps = new ArrayList<NID>();	
			lb = new LoadBalancer();
			
			// Config RM
			File f = new File(RM_config_file_name);
			FileReader fis = null;
			try {
				fis = new FileReader(f);
			} catch (FileNotFoundException e) {
				Log.pw("RM", String.format("RM Config File Not Found \"%s\"", RM_config_file_name));
			}
			
			InetAddress FE_addr = null, ferm_mc_addr = null, rmrs_mc_addr = null;
			int FE_port = 0, ferm_mc_port = 0, rmrs_mc_port = 0, local_port = 0;
			
			BufferedReader br = new BufferedReader(fis);
			try {
				String line = br.readLine();
				while(line != null){

					switch(line){
					case Res.config_RM_udp_port_string:
						line = br.readLine();
						local_port = Integer.parseInt(line);
						break;
					case Res.config_FE_udp_addr_string:
						line = br.readLine();
						FE_addr = InetAddress.getByName(line);
						break;
					case Res.config_FE_udp_port_string:
						line=br.readLine();
						FE_port = Integer.parseInt(line);
						break;
					case Res.config_FE_RM_mc_addr_string:
						line=br.readLine();
						ferm_mc_addr = InetAddress.getByName(line);
						break;					
					case Res.config_FE_RM_mc_port_string:
						line=br.readLine();
						ferm_mc_port = Integer.parseInt(line);
						break;	
					case Res.config_RM_RS_mc_addr_string:
						line=br.readLine();
						rmrs_mc_addr = InetAddress.getByName(line);
						break;					
					case Res.config_RM_RS_mc_port_string:
						line=br.readLine();
						rmrs_mc_port = Integer.parseInt(line);
						break;	
					default:

					}
					line = br.readLine();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			
			
			FE = new NID(FE_addr, FE_port);
			rmrs_mc = new NID(rmrs_mc_addr, rmrs_mc_port);
			ferm_mc = new NID(ferm_mc_addr, ferm_mc_port);
			

			try {
				local_nid = new NID(InetAddress.getLocalHost(), local_port);
			} catch (UnknownHostException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}
	
	private void joinGroup() throws IOException{
		// IMPORTANT
		rmrs_mc_socket.setInterface(InetAddress.getLocalHost()); 
		rmrs_mc_socket.setTimeToLive (TTL);
		rmrs_mc_socket.joinGroup(rmrs_mc.addr);
		
		Log.pw("RM", "Joined " + rmrs_mc);

		ferm_mc_socket.setInterface(InetAddress.getLocalHost()); 
		ferm_mc_socket.setTimeToLive (TTL);
		ferm_mc_socket.joinGroup(ferm_mc.addr);
		
		Log.pw("RM", "Joined " + ferm_mc);	
	}
	
	private void contactFE(){
		//Register RM
		//"RM_FE_register" + ":"
		Tools.send(udp_socket, 
				new Message().
				put("content", Res.RM_FE_register_string + Res.separator).
				put("rm_nid",  local_nid),
				FE.addr, FE.port);
		
		Message receive = (Message) Tools.convertToObject(Tools.receive(udp_socket).getData());
		
		rm_udps = (TreeMap<Timestamp, NID>) receive.get("rm_list");
		local_timestamp = (Timestamp) receive.get("joined_timestamp");
	}
	
	class LoadBalancer{
		private Random r = new Random();
		
		public NID balancedLocation(String file_name){
			if(rs_udps.size() == 0)
				return null;
			int index = r.nextInt() % rs_udps.size();
			while(index < 0)
				index += rs_udps.size();
			return rs_udps.get(index);
		}
	}
	
	class HeartbeatThread implements Runnable{
		
		private boolean go = true;
		private boolean terminated = false;
		
		public synchronized void setGo(boolean _go){
			go = _go; 
			rm_udp_receive_runnable.resumeSafetyChecker();
			}
		public synchronized boolean isGo(){	return go;}
		
		public synchronized void terminate(){ terminated = true; }
		public synchronized boolean isTerminated(){ return terminated; }

		@Override
		public void run() {
			while(!isTerminated()){
				if(go){
					Message to_next_message = new Message()
								.put("content", Res.heartbeat + Res.separator)
								.put("NID0", local_nid)
								.put("index", 1);

					NID next_nid = getNextNid();

					//Log.pw("RM_HB", "starting heartbeat to next: " + next_nid);
					Tools.send(udp_socket, to_next_message, next_nid.addr, next_nid.port);	


					// 0.5s
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						Thread.currentThread().interrupt();
					}

				}
				else{
					synchronized(this){
						try {
							Log.pw("RM", "heartbeat paused");
							this.wait();
							Log.pw("RM", "heartbeat restarted");
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
				

				Thread.yield();
			}

		}
	}
	
	class PingAckAllChecker implements Runnable{

		@Override
		public void run() {
			// TODO Auto-generated method stub
			for(NID nid: rm_udps.values()){
				Tools.send(udp_socket, new Message().put("content", Res.ping + Res.separator), nid.addr, nid.port);
				Log.pw("RM", "pinged " + nid);
			}
			
			
			try {
				Thread.sleep(5000L);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				Thread.currentThread().interrupt();
			}	
			
			ArrayList<NID> alives = rm_udp_receive_runnable.getAlive();

			//Log.pw("RM", "alive list: " + alives);
			
			Message to_fe_message = new Message()
						.put("content", Res.update_rm_list_string + Res.separator)
						.put("alives", alives);
			Tools.send(udp_socket, to_fe_message, FE.addr, FE.port);

			//Log.pw("RM", "sent alive list to FE: " + FE);
			//send to FE;	
		}
	}

}
