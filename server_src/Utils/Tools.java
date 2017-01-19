package Utils;




import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class Tools {
	
	//Utility Functions
	public static void send(
			DatagramSocket socket, 
			Serializable s, 
			InetAddress addr, 
			int port){
		byte[] data = Tools.convertToByteArray(s);
		DatagramPacket send_packet = new DatagramPacket(
				data,
				data.length,
				addr,
				port
				);

		try {
			socket.send(send_packet);
		} catch (IOException e) {
			Log.print("IOException while sending");}
	}

	public static DatagramPacket receive(DatagramSocket socket){
		DatagramPacket receive_packet = new DatagramPacket(
				new byte[Res.buf_len],
				Res.buf_len);
		try {
			socket.receive(receive_packet);
		} catch (IOException e) {
			Log.print("IOException while receiving");
		}

		return receive_packet;
	}
	
	public static byte[] convertToByteArray(Object o){
		byte[] data_arr = null;
		try{
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(o);
			data_arr = bos.toByteArray();
			bos.close(); oos.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		return data_arr;
	}
	
	public static Object convertToObject(byte[] data){
		Object o = null;
		try{
			ByteArrayInputStream bis = new ByteArrayInputStream(data);
			ObjectInputStream ois = new ObjectInputStream(bis);
			o = ois.readObject();
			bis.close(); ois.close();
		}catch(IOException e){
			e.printStackTrace();
		}catch(ClassNotFoundException e){
			e.printStackTrace();
		}
		return o;
	}
	
	public static InetAddress getAddress(String response) throws UnknownHostException{

		//System.out.printf("response1 = %s\n", response);
		InetAddress TCP_addr = null;
		while(response.contains("/"))
			response = response.substring(1);
		
		//System.out.printf("response2 = %s\n", response);
		int i = response.indexOf(Res.separator);
		//System.out.printf("addr = %s\n", response.substring(0, i));
		TCP_addr = InetAddress.getByName(response.substring(0, i));
		
		return TCP_addr;
	}
	
	public static int getPort(String response){
		int i = response.indexOf(Res.separator);
		return Integer.parseInt(response.substring(i+1));
	}

}
