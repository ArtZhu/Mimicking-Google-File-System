package frontEnd;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import Utils.Log;
import Utils.Message;
import Utils.NID;
import Utils.Res;
import Utils.TimestampComparator;
import Utils.Tools;

public class FE {	
	private TreeMap<Timestamp, NID> RM_list;
	
	private DatagramSocket udp_socket;
	private NID udp;
	
	private MulticastSocket mc_socket;
	private NID mc;
	
	public FE(){
		
		try {
			new FrontEndConfigurer().configure();
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		//init socket
		try {
			udp_socket = new DatagramSocket(udp.port);
			mc_socket = new MulticastSocket(mc.port);
		} catch (SocketException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
	
	public void start(){
		Thread listen = new Thread(new FE_UDP_ReceiveThread(udp_socket, this));
		listen.setDaemon(true);
		listen.start();
		
		Thread t = new Thread(new FE_MC_ReceiveSupervisorThread(mc_socket));
		t.setDaemon(true);
		t.start();
		
		try{
			listen.join();
			t.join();
		}catch(InterruptedException e){}
	}
	
	public synchronized void registerRM(NID rm){
		Timestamp ts = new Timestamp(Calendar.getInstance().getTimeInMillis());
		RM_list.put(ts, rm);
		
		Message to_rm_message = new Message()
				.put("rm_list", RM_list)
				.put("joined_timestamp", ts);
		Tools.send(udp_socket, to_rm_message, rm.addr, rm.port);
	}
	
	public synchronized NID getLocation(String file_name){
		return RM_list.get(RM_list.keySet().iterator().next());
	}
	
	public synchronized void broadcastRMs(){
		Message broadcast_message = new Message()
		.put("content", Res.update_rm_list_string + Res.separator)
		.put("rm_list", RM_list);


		Log.pw("FE", "FE sending rm list: " + RM_list.keySet());
		Tools.send(mc_socket, 
				broadcast_message, mc.addr, mc.port);
	}
	
	public synchronized void updateRMs(ArrayList<NID> alives){

		Log.print("alives: " + alives);
		ArrayList<Map.Entry<Timestamp, NID>> entryset = new ArrayList<Map.Entry<Timestamp, NID>>();
		entryset.addAll(RM_list.entrySet());
	
		
		
		for(Map.Entry<Timestamp, NID> entry: entryset){
			System.out.println(entry.getValue() + "\n\n" + alives);
			if(!alives.contains(entry.getValue())){
				RM_list.remove(entry.getKey());
			}
		}

		Collection<NID> nids = RM_list.values();
		alives.removeAll(nids);
		Log.print("after remove: " + alives);
	
		int i = 0;
		for(NID nid: alives){
			RM_list.put(new Timestamp(Calendar.getInstance().getTimeInMillis() - 100L * i), nid);
			i++;
		}
		
		
		Message broadcast_message = new Message()
					.put("content", Res.update_rm_list_string + Res.separator)
					.put("rm_list", RM_list);
		

		Log.pw("FE", "FE found alives: " + RM_list.values());
		Tools.send(mc_socket, 
				broadcast_message, mc.addr, mc.port);
		Log.pw("FE", "FE broadcasted alive list.");
	}
	
//////////////////////////////////////////////////////////////////////////////////////////
	
	private interface HashManager{
		public int hash(String file_name);
	}

	private class Mod3Hash implements HashManager{
		
		
		public int hash(String file_name){
			Log.pw("RM", String.format("Hashed file \"%s\"", file_name));
			int sum = 0;
			for(int i=0; i<file_name.length(); i++)
				sum += file_name.charAt(i) - 'a' + 1;
			
			Log.pw("RM", String.format("sum = %d; value = %d", sum, sum % 3));
			return sum % RM_list.size();
		}
	}
	
//////////////////////////////////////////////////////////////////////////////////////////
	
	private String FE_config_file_name = "FEConfig";
	private class FrontEndConfigurer{
		
		public void configure() throws UnknownHostException{
			RM_list = new TreeMap<Timestamp, NID>(new TimestampComparator());
			
			// Config FE
			File f = new File(FE_config_file_name);
			FileReader fis = null;
			
			InetAddress udp_addr = null, mc_addr = null;
			int udp_port = 0, mc_port = 0;
			
			udp_addr = InetAddress.getLocalHost();
			
			try {
				fis = new FileReader(f);
			} catch (FileNotFoundException e) {
				Log.pw("FrontEnd", String.format("FrontEnd Config File Not Found \"%s\"", FE_config_file_name));
			}
			
			BufferedReader br = new BufferedReader(fis);
			try {
				String line = br.readLine();
				while(line != null){
					//
					switch(line){
					case Res.config_FE_udp_port_string:
						line=br.readLine();
						udp_port = Integer.parseInt(line);
						break;
					case Res.config_FE_RM_mc_port_string:
						line=br.readLine();
						mc_port = Integer.parseInt(line);
						break;
					case Res.config_FE_RM_mc_addr_string:
						line=br.readLine();
						mc_addr = InetAddress.getByName(line);
						break;
					default:
						
					}				
					line = br.readLine();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			mc = new NID(mc_addr, mc_port);
			udp = new NID(udp_addr, udp_port);
		}
	}
	
}





