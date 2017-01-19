package resourceServer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.MulticastSocket;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Scanner;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

import Utils.Message;
import Utils.Res;

public class RS_MC_FileThread implements Runnable {
	private String file_name;
	private RS	rs;

	public RS_MC_FileThread(String _file_name, RS _rs){
		file_name = _file_name;
		rs = _rs;
	}
	
	@Override
	public void run() {
		String line;
		Lock rl = rs.file_locks().get(file_name).readLock();
		File f = new File(Res.file_path + file_name);
		Scanner sc = null;
		try{
			rl.lock();
			try {
				sc = new Scanner(f);
			} catch (FileNotFoundException e) {	e.printStackTrace(); }

			int i=0;
			while(sc.hasNextLine()){
				line = sc.nextLine();
				Message mc_message = new Message().
						put("content", Res.update_file_string + Res.separator + file_name).
						put("line", line).
						put("tag", i).put("done", !sc.hasNextLine());
				i++;
				rs.broadcast(mc_message);
			}
		}finally{
			rl.unlock();
		}
	}
}
