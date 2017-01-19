package Utils;

import java.io.Serializable;
import java.net.InetAddress;


public class NID implements Serializable{
	public final InetAddress 	addr;
	public final Integer		port;
	
	public NID(InetAddress _addr, Integer _port){
		addr = _addr; port = _port;
	}

	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof NID)){
			return false;
		}
		
		NID nid = (NID) obj;
		
		
		return  nid.addr.equals(this.addr) && nid.port.equals(port);
	}

	@Override
	public String toString() {
		return addr.toString() + Res.separator + port;
	}

}
