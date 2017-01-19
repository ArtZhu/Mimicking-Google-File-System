package __program;
import Utils.Log;
import resourceManager.RM;




public class StartRM{
	
	public static void main(String[] args){
		Log.pw("RM", "Starting RM!");
		RM r = new RM();
		r.start();	
	}
}
