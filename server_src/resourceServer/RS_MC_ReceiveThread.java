package resourceServer;
import java.net.DatagramPacket;
import java.net.MulticastSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import Utils.Log;
import Utils.Message;
import Utils.Res;
import Utils.Tools;


public class RS_MC_ReceiveThread implements Runnable{
	private MulticastSocket mc_socket;
	private RS rs;
	
	private Map<String, ArrayList<Message>> file_update_queues = new HashMap<String, ArrayList<Message>>();
	
	public RS_MC_ReceiveThread(MulticastSocket _mc_socket, RS _rs){
		mc_socket = _mc_socket;
		rs = _rs;
	}

	@Override
	public void run() {
		DatagramPacket receive_packet;
		Message receive;
		String content, request, info;

		while(true){
			receive_packet = Tools.receive(mc_socket);
			//Log.pw("ResourceServer", "RS_MC Received Something");
			
			receive = (Message) Tools.convertToObject(receive_packet.getData());

			content = (String) receive.get("content");

			//Log.pw("RM", "Message content: " + content);

			int i = content.indexOf(Res.separator);
			request = content.substring(0, i);
			info = content.substring(i+1);

			switch(request){
			case Res.update_file_string:
				ArrayList<Message> queue = file_update_queues.get(info);
				if(queue == null){
					queue = new ArrayList<Message>();
				}
				queue.add(receive);
				file_update_queues.put(info, queue);
				if((Boolean) receive.get("done")){
					Log.pw("RS_MC", String.format("File transfer done: %s", file_update_queues.get(info)));
					file_update_queues.remove(info);
					rs.updateFile(queue);	//
				}
				break;
				
			default:
				break;
			}
			
			Thread.yield();
		}
		
	}
	
}