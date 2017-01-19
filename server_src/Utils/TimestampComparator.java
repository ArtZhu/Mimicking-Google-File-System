package Utils;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Comparator;


public class TimestampComparator implements Comparator<Timestamp>, Serializable {

	@Override
	public int compare(Timestamp o1, Timestamp o2) {
		if(o1.before(o2)){
			return -1;
		}
		else if(o1.equals(o2)){
			return 0;
		}else{
			return 1;
		}
		
	}

}
