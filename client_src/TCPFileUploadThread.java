
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.util.Scanner;

import Util.Log;
import Util.Res;

public class TCPFileUploadThread extends TCPFileThreadSkeleton{
	private Scanner sc;
	private String file_name;
	
	
	public TCPFileUploadThread(InetAddress _addr, int _port, String _file_name){
		super(_addr, _port, _file_name);
		
		file_name = _file_name;
		
		try {
			if(!f.exists()){
				System.out.println("file %s does not exist!");
				return;
			}
				
			sc = new Scanner(f);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		};
	}
	
	@Override
	public void run() {
		try {
			ObjectOutputStream oos = new ObjectOutputStream(TCP_socket.getOutputStream());
			oos.writeObject(Res.upload_string + Res.separator + file_name);
			
			String s = null;
			while(sc.hasNextLine()){
				String line = sc.nextLine() + "\n";
				while(line.length() > 80){
					oos.writeObject(line.substring(0, 80));
					line = line.substring(80);
					Log.write("Client_v1", String.format("Sent %s", s));
				}
				oos.writeObject(line);
				Log.write("Client_v1", String.format("Sent %s", s));
			}
			
			oos.writeObject(Res.EOF);
			
			Log.pw("Client_v1", "Finished uploading");
			
			oos.close(); sc.close(); TCP_socket.close();
		} catch (IOException e) {}
		
	}
}
