package resourceServer;

import java.io.BufferedInputStream;
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
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

import Utils.Log;
import Utils.Message;
import Utils.NID;
import Utils.Res;
import Utils.Tools;

public class RS{

	private List<NID> rs_list;
	private Timestamp rs_list_update_ts;
	public synchronized Timestamp getRSListUpdateTS()	{	return rs_list_update_ts;	}
	public synchronized void setRSListUpdateTS(Timestamp _rs_list_update_ts) {	rs_list_update_ts = _rs_list_update_ts;	}
	public synchronized List<NID> getRSList()	{	return rs_list;	}
	public synchronized void setRSUDPs(List<NID> _rs_list) {	rs_list = _rs_list;	}

	public NID RM;
	
	private NID local_nid;
	private DatagramSocket 	udp_socket;
	private	int				port = 8819;

	/*	multicast	*/
	private NID					mc;
	private MulticastSocket		mc_socket;
	private int 				TTL = 10;

	private HashMap<String, ReadWriteLock> file_locks = new HashMap<String, ReadWriteLock>();
	private HashMap<String, Timestamp>		last_updates = new HashMap<String, Timestamp>();
	public 	HashMap<String, Timestamp> 		last_updates(){ return last_updates; }
	public 	HashMap<String, ReadWriteLock> 	file_locks(){ return file_locks; }

	public RS(){

		new ResourceServerConfigurer().configure();

		//contact RM
		if(!contactRM())
			return;

		try {
			join_group();
		} catch (IOException e1) {
			Log.pw("RS", "IOException while joining multicast group");
		}

	}
	
	public void start(){
		Thread udp_receive_thread = new Thread((new RS_UDP_ReceiveThread(udp_socket, this)));
		udp_receive_thread.setDaemon(true);
		udp_receive_thread.start();

		Thread mc_receive_thread = new Thread(new RS_MC_ReceiveThread(mc_socket, this));
		mc_receive_thread.setDaemon(true);
		mc_receive_thread.start();

		try {
			udp_receive_thread.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/* register */
	public boolean registerServer(NID rs_udp, NID rs_hb){
		if(rs_list.size() >= 3)
			return false;
		if(!rs_list.contains(rs_udp)){
			rs_list.add(rs_udp);
		}				
		return true;
	}

	/* unregister */
	public boolean unregisterServer(NID rs_udp, NID rs_hb){
		return rs_list.remove(rs_udp);
	}

	/* election */
	public void startElection(){
		//Implement!
	}

	/* replica */
	public void replica(String file_name){
		new Thread(new RS_MC_FileThread(file_name, this)).start();
	}
	
	/* replica */
	public void replicaAccordingTo(HashMap<String, Timestamp> file_updates){
		ArrayList<Map.Entry<String, Timestamp>> updates = new ArrayList<Map.Entry<String, Timestamp>>(last_updates.entrySet());
		for(Map.Entry<String, Timestamp> entry: updates){
			if(file_updates.get(entry.getKey()) == null || file_updates.get(entry.getKey()).before(entry.getValue())){
				replica(entry.getKey());
			}
		}
	}

	/* join group */
	private void join_group() throws IOException{
		//mc_socket = new MulticastSocket(mc.port);

		// IMPORTANT
		mc_socket.setInterface(InetAddress.getLocalHost()); 

		mc_socket.setTimeToLive (TTL);
		mc_socket.joinGroup(mc.addr);
		Log.pw("RS", "Joined " + mc);
	}

	/*	broadcast	*/
	public void broadcast(Serializable s){
		Tools.send(mc_socket, s, mc.addr, mc.port);
	}
	
	/* send UDP */
	public void sendUDP(Serializable s, InetAddress addr, int port){
		Tools.send(udp_socket, s, addr, port);
	}

	//public void writeFile(String file_name)

	public void updateFile(ArrayList<Message> queue){
		Log.write("ResourceServer", "RS sorting queue: " + queue);
		queue.sort(new Comparator<Message>(){
			@Override
			public int compare(Message o1, Message o2) {
				return (Integer) o1.get("tag") - (Integer) o2.get("tag");
			}	
		});


		Log.write("ResourceServer", "RS sorted queue: " + queue);

		String content = (String) queue.get(0).get("content");
		int i = content.indexOf(Res.separator);
		String file_name = content.substring(i+1);

		Timestamp update_timestamp = queue.get(queue.size() - 1).timestamp;
		Log.pw("ResourceServer", 
				String.format("Current update at: %s\nLast update at %s", 
						update_timestamp, last_updates.get(file_name)));


		File f = this.openFile(file_name);


		if(update_timestamp.before(last_updates.get(file_name))){
			Log.pw("ResourceServer", "Last update happened after this");
			return;
		}

		ReadWriteLock rwl = file_locks.get(file_name);
		rwl.readLock().lock();
		Log.pw("Resource Server", "Obtained ReadLock on " + file_name);
		try{
			FileWriter fw = null;
			rwl.readLock().unlock();

			Log.pw("Resource Server", "Released ReadLock on " + file_name);

			rwl.writeLock().lock();

			Log.pw("Resource Server", "Obtained WriteLock on " + file_name);
			try {
				fw = new FileWriter(f);
				fw.write("");
				while(!queue.isEmpty()){
					Message m = queue.remove(0);
					Log.write("ResourceServer", "appended line: " + m.get("line"));
					fw.append((String) m.get("line") + "\n");
				}
				fw.flush();
				fw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}finally{
			rwl.writeLock().unlock();

			Log.pw("Resource Server", "Released WriteLock on " + file_name);
		}
	}

	private NID primary_udp;
	private NID primary_hb;
	public void setPrimaryUDP(NID _primary_udp){ primary_udp = _primary_udp;}
	public void setPrimaryHB(NID _primary_hb){ primary_hb = _primary_hb;}


	public boolean contactRM(){
		Message to_rm_message = null, receive_message;
		String receive, ack;

		try {
			to_rm_message = new Message().
					put("content", Res.RS_RM_join_as_server_request + Res.separator).
					put("rs_udp", new NID(InetAddress.getLocalHost(), udp_socket.getLocalPort()));
			//Log.pw("ResourceServer", String.format("I'm joining as ", ));
		} catch (UnknownHostException e) {
		}

		Log.pw("ResourceServer", "Requesting to join as a Server");
		Tools.send(udp_socket, 
				to_rm_message , RM.addr, RM.port);


		Log.pw("ResourceServer", "Listening for ack string");
		receive_message = (Message) Tools.convertToObject(Tools.receive(udp_socket).getData());
		ack = (String) receive_message.get("content");		

		if(ack.contains(Res.RM_RS_join_as_server_confirm)){
			Log.pw("ResourceServer", "I am a server now!");
			rs_list = (ArrayList<NID>) receive_message.get("rs_udps");
		
			return true;
		}else{
			Log.write("ResourceServer", "Didn't join as server, system exiting");
			return false;
		}
	}

	public synchronized File openFile(String file_name){
		File f = new File(Res.file_path + file_name);
		if(!f.exists()){
			try {
				f.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
			this.file_locks.put(file_name, new ReentrantReadWriteLock());
			this.last_updates.put(file_name, new Timestamp(date.getTime() - 100000L));
		}
		return f;
	}

	private final String RS_config_file_name = "RSConfig";
	class ResourceServerConfigurer{

		public void configure(){
			// Config RM
			File f = new File(RS_config_file_name);
			FileReader fis = null;
			try {
				fis = new FileReader(f);
			} catch (FileNotFoundException e) {
				Log.pw("RS", String.format("RM Config File Not Found \"%s\"", RS_config_file_name));
			}

			InetAddress RM_addr = null, mc_addr = null;
			int RM_port = 0, mc_port = 0;

			BufferedReader br = new BufferedReader(fis);
			try {
				String line = br.readLine();
				while(line != null){

					switch(line){
					case Res.config_RM_udp_addr_string:
						line = br.readLine();
						RM_addr = InetAddress.getByName(line);
						break;
					case Res.config_RM_udp_port_string:
						line = br.readLine();
						RM_port = Integer.parseInt(line);
						break;
					case Res.config_RM_RS_mc_addr_string:
						line=br.readLine();
						mc_addr = InetAddress.getByName(line);
						break;					
					case Res.config_RM_RS_mc_port_string:
						line=br.readLine();
						mc_port = Integer.parseInt(line);
						break;	
					default:
					}
					line = br.readLine();
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try{
					br.close(); fis.close();
				} catch(IOException e) {
					
				}
			}
			
			

			RM = new NID(RM_addr, RM_port);
			mc = new NID(mc_addr, mc_port);

			//Init socket
			try {
				udp_socket = new DatagramSocket();
				mc_socket = new MulticastSocket(mc.port);
			} catch (SocketException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			

			try {
				local_nid = new NID(InetAddress.getLocalHost(), udp_socket.getLocalPort());
			} catch (UnknownHostException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			File folder = new File(Res.file_path);
			for(File file: folder.listFiles()){
				String file_name = file.getName();
				file_locks.put(file_name, new ReentrantReadWriteLock());
				last_updates.put(file_name, new Timestamp(date.getTime() - 100000L));
			}
		}
	}

	private Date date = new Date();
}
