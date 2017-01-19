package __program;
import Utils.Log;
import frontEnd.FE;





public class StartFE {

	public static void main(String[] args){
		Log.pw("FrontEnd", "Starting FrontEnd!");
		FE fe = new FE();
		fe.start();
	}
}
