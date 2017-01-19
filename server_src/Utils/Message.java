package Utils;



import java.io.Serializable;
import java.net.InetAddress;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Message implements Serializable {
	public final Timestamp timestamp;
	private Map<String, Serializable> extra = new HashMap<String, Serializable>();
	
	private Date date = new Date();
	
	public Message(){
		timestamp = new Timestamp(date.getTime());
	}
	
	public Message put(String key, Serializable value){
		extra.put(key, value);
		return this;
	}
	
	public Serializable get(String key){
		return extra.get(key);
	}
	
	public String toString(){
		String ans = String.format("Message generated at %s:\n", timestamp.toString());
		for(Map.Entry<String, Serializable> entry: extra.entrySet()){
			ans += "<K>" + entry.getKey() + "<V>" + entry.getValue() + "\n";
		}
		return ans;
	}
}
