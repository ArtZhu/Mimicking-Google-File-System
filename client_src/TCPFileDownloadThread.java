import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;

import Util.Log;
import Util.Res;


public class TCPFileDownloadThread extends TCPFileThreadSkeleton{
	private FileWriter fw;
	private String file_name;
	
	public TCPFileDownloadThread(InetAddress _addr, int _port, String _file_name){
		super(_addr, _port, _file_name);
		
		file_name = _file_name;
		
		try {
			if(!f.exists())
				f.createNewFile();
			fw = new FileWriter(f);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		};
	}
	
	@Override
	public void run() {
		try {
			ObjectOutputStream oos = new ObjectOutputStream(TCP_socket.getOutputStream());
			oos.writeObject(Res.download_string + Res.separator + file_name);
			
			ObjectInputStream ois = new ObjectInputStream(TCP_socket.getInputStream());
			
			String s = null;
			while(true){
				try {
					s = (String) ois.readObject();
				} catch (ClassNotFoundException e) {}
				
				if(s.contains(Res.EOF))
					break;
				
				Log.write("Client_v1", String.format("Read %s", s));
				fw.write(s);
			}
			
			Log.pw("Client_v1", "Finished downloading");
			
			ois.close(); fw.close(); TCP_socket.close();
		} catch (IOException e) {}
		
	}

}
