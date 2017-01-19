package frontEnd;
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
import Utils.Tools;


public class FE_MC_ReceiveSupervisorThread implements Runnable{
	private MulticastSocket mc_socket;
	
	public FE_MC_ReceiveSupervisorThread(MulticastSocket _mc_socket){
		mc_socket = _mc_socket;
	}

	@Override
	public void run() {
		DatagramPacket receive_packet;
		Message receive;
		String content, request, info;

		while(true){
			Log.pw("FE_MC", "Trying to receive something from FERM");
			receive_packet = Tools.receive(mc_socket);
			//Log.pw("ResourceServer", "RS_MC Received Something");
			
			receive = (Message) Tools.convertToObject(receive_packet.getData());

			Log.pw("FE", "Multicast message: " + receive);
		
			Thread.yield();
		}
		
	}
	
}