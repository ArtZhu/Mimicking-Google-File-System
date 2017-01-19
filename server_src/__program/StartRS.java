package __program;
import Utils.Log;
import resourceServer.RS;




public class StartRS{

	public static void main(String[] args) {
		Log.pw("ResourceServer", "Starting ResourceServer");
		RS s = new RS();
		s.start();
	}
}
