package Utils;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.sql.Timestamp;
import java.util.Date;


public class Log {
	private static final boolean file = true;
	private static final boolean print = true;
	
	private static File log_file;
	private static FileWriter fw = null;
	
	private static Date d;
	
	public static void pw(String class_name, String log){
		print(class_name + " : " + log);
		write(class_name, log);
	}
	
	public static void print(String log){
		d = new Date();
		log = new Timestamp(d.getTime()) + "\n\t\t" + log;
		if(print)
			System.out.println(log);
	}
	
	public static void write(String class_name, String log){
		Date d = new Date();
		log = d.toString() + Res.separator + log;
		try {
			_init(class_name);
			if(file)
				fw.write(log + "\n");
			fw.flush();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void _init(String class_name) {
		try{
			String file_name = class_name + Res.log_file_name +
					d.getMonth() + "_" + d.getDate();
			log_file = new File(file_name);
			if(!log_file.exists())
				log_file.createNewFile();
			fw = new FileWriter(log_file, true);
		}catch(IOException e){

		}
	}
}
