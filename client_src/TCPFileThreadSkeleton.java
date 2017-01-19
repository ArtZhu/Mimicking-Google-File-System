import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.Socket;

import Util.Log;
import Util.Res;

public abstract class TCPFileThreadSkeleton implements Runnable{
	protected InetAddress addr;
	protected int			port;
	
	protected Socket TCP_socket;
	
	protected File f;
	
	public TCPFileThreadSkeleton(InetAddress _addr, int _port, String file_name){
		addr = _addr;
		port = _port;
		try {
			TCP_socket = new Socket(addr, port);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		f = new File(Res.file_path + file_name);
	}
	
	@Override
	public abstract void run();

}
