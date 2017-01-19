package resourceServer;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;
import java.util.concurrent.locks.ReadWriteLock;

import Utils.Log;
import Utils.Res;


public class RS_TCP_FileConnectionThread implements Runnable{
	protected ServerSocket server_socket;
	private RS rs;
	
	public RS_TCP_FileConnectionThread(ServerSocket _server_socket, RS _rs){
		server_socket = _server_socket;
		rs = _rs;
	}

	@Override
	public void run() {
		try {
			Log.pw("ResourceServer", "TCP connection thread listening for connection");
			Socket s = server_socket.accept();
			
			ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
			ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
			
			String request = (String) ois.readObject();
			int i = request.indexOf(Res.separator);
			String req_type = request.substring(0, i);
			String file_name = request.substring(i+1);

			Log.pw("ResourceServer", 
					String.format("TCP got request %s for file %s", req_type, file_name));
			
			File f = rs.openFile(file_name);			
			
			if(req_type.equals(Res.upload_string)){
				ReadWriteLock rwl = rs.file_locks().get(file_name);					
				rwl.writeLock().lock();
				try{
					FileWriter fw = new FileWriter(f);
					fw.write("");
					String line = (String) ois.readObject();
					while(!line.equals(Res.EOF)){
						Log.write("ResourceServer", 
								String.format("TCP got line %s", line));
						fw.append(line);
						line = (String) ois.readObject();
					}
					Log.pw("ResourceServer", "Finished writing");
					rs.replica(file_name);
					fw.close();
				}finally{
					rwl.writeLock().unlock();
				}
			}else{
				ReadWriteLock rwl = rs.file_locks().get(file_name);
				rwl.readLock().lock();
				try{
					Scanner sc = new Scanner(f);
					while(sc.hasNextLine()){
						String line = sc.nextLine() + "\n";
						while(line.length() > 80){
							oos.writeObject(line.substring(0, 80));
							line = line.substring(80);
							Log.write("ResourceServer", 
									String.format("TCP wrote %s", line));
						}
						oos.writeObject(line);
						Log.write("ResourceServer", 
								String.format("TCP wrote %s", line));
					}
					oos.writeObject(Res.EOF);	
					Log.write("ResourceServer", 
							String.format("TCP got EOF"));

					Log.pw("ResourceServer", "Finished reading");
					sc.close();
				}finally{
					rwl.readLock().unlock();
				}
			}

			oos.close(); ois.close();s.close(); server_socket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}

}
