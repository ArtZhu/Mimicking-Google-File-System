package resourceServer;



import java.io.File;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import Utils.Log;
import Utils.Res;

public class FileManager implements Comparator<String>{
	
	private static String time_regex_converted = "(\\d+\\_){7}";
	
	private static String filename_ts_starter = "ts";
	private static Pattern time_regex_pattern;
	
	private static String timestamp_separators_regex = "[- :\\.]";

	public static void main(String[] args) throws NoTimestampInFilenameException{
		//File f1 = new File("2016_05_13_10_24_31_43_fred.txt");
		//File f2 = new File("2016_05_13_10_34_13_145_emma.txt");
		
		System.out.println(new FileManager().compare("emma.txt", "fred.txt"));
		FileManager.cleanUp("emma.txt");
	}
	
	public static void cleanUp(String filename) throws NoTimestampInFilenameException{
		File winner = null;
		for(File f: listFiles(Res.file_path)){
			if(f.getName().contains(filename)){
				if(winner == null)
					winner = f;
				else{
					if(getTimestamp(winner).before(getTimestamp(f))){
						winner.delete();
						winner = f;
					}
				}
			}
		}
	}
	
	@Override
	public int compare(String o1, String o2) {
		File f1, f2;
		f1 = new File(o1); f2 = new File(o2);
		
		Timestamp ts1 = null, ts2 = null;
		
		try {
			ts1 = getTimestamp(f1);
		} catch (NoTimestampInFilenameException e) {
			Log.pw("Files","Filename does not contain timestamp" + f1.getName());
			return 0;
		}
		try {
			ts2 = getTimestamp(f2);
		} catch (NoTimestampInFilenameException e) {
			Log.pw("Files","Filename does not contain timestamp" + f2.getName());
			return 0;
		}
		
		if(ts1.before(ts2))
			return -1;
		else
			return 1;
	}
	
	public static void convertFile(File f){
		String new_name = 
				toFilename(new Timestamp(Calendar.getInstance().getTimeInMillis())) + f.getName();
		f.renameTo(new File(new_name));
	}

	
	
	public static String toFilename(Timestamp ts){
		return toFilename(ts.toString());
	}
	
	public static String toFilename(String ts){
		return ts.replaceAll(timestamp_separators_regex, "_") + "_";
	}
	
	public static Timestamp getTimestamp(File f) throws NoTimestampInFilenameException{
		return getTimestamp(f.getName());
	}
	
	public static Timestamp getTimestamp(String filename) throws NoTimestampInFilenameException{
		if(time_regex_pattern == null)
			time_regex_pattern = Pattern.compile(time_regex_converted);
		
		Matcher m = time_regex_pattern.matcher(filename);
		if(m.find()){
			String s = m.group();
			s = s
			.replaceFirst("_", "-")
			.replaceFirst("_", "-")
			.replaceFirst("_", " ")
			.replaceFirst("_", ":")
			.replaceFirst("_", ":")
			.replaceFirst("_", ".")
			.replaceFirst("_", "");
			return Timestamp.valueOf(s);
		}
		throw new NoTimestampInFilenameException();
	}
	
	
	public static File[] listFiles(String path){
		File folder = new File(path);
		return folder.listFiles();
	}
	
	public static class NoTimestampInFilenameException extends Exception{
		/**
		 * 
		 */
		private static final long serialVersionUID = -513248466267399661L;
		
	}
}
