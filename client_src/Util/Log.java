package Util;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Date;


public class Log {
	private static final boolean file = true;
	private static final boolean print = true;
	
	private static File log_file;
	private static FileWriter fw = null;
	
	public static void pw(String class_name, String log){
		print(log);
		write(class_name, log);
	}
	
	public static void print(String log){
		Date d = new Date();
		log = d.toString() + Res.separator + log;
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
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void _init(String class_name) throws IOException{
		log_file = new File(class_name + Res.log_file_name);
		fw = new FileWriter(log_file, true);
	}
}
