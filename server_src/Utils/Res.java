package Utils;



import java.util.regex.Pattern;

public class Res {
	public static final String 	EOF				= "EOF!!!";
	
	public static final String	separator 	= ":";
	
	public static final String 	upload_string 	= "upload file";
	public static final String 	download_string 	= "download file";
	public static final String 	file_loc_request_string = "Request Location of";
	
	public static final String 	RS_RM_join_as_server_request = "Requesting to join as server";
	public static final String 	RM_RS_join_as_server_confirm = "You are a server now!";
	public static final String 	RM_RS_join_as_server_reject = "You failed to be a server!";
	
	public static final String 	RM_RS_open_TCP_request_string = "Open a TCP!";
	
	public static final int		buf_len			= 2048;
	
	public static final String	log_file_name 	= "log";
	
	public static final String 	file_path 		= "testfiles/";
	
	//Apr 4th, 2016
	public static final String 	RM_FE_register_string = "RMishere";
	
	public static final String	file_loc_found_string = "fileFound";
	
	public static final Pattern addr_port_pattern = Pattern.compile("\\d+\\.\\d\\.\\d+\\.\\d+\\:\\d+");
	
	//config
	public static final String 	config_FE_udp_port_string = "FE_udpport";
	public static final String 	config_FE_udp_addr_string = "FE_udpaddr";
	public static final String	config_FE_RM_mc_port_string = "FE_RM_mcport";
	public static final String 	config_FE_RM_mc_addr_string = "FE_RM_mcaddr";
	public static final String 	config_RM_udp_port_string = "RM_udpport";
	public static final String 	config_RM_udp_addr_string = "RM_udpaddr";
	public static final String	config_RM_RS_mc_port_string = "RM_RS_mcport";
	public static final String 	config_RM_RS_mc_addr_string = "RM_RS_mcaddr";


	public static final String	RS_client_TCP_string = "connectTCP";
	
	public static final String  new_rs_string = "newRS";
	public static final String  remove_rs_string = "removeRS";
	
	public static final String	register_heartbeat_string = "getmein";
	
	public static final String	heartbeat = "BOP";
	
	public static final String	RS_RS_set_target_string = "setTarget";
	
	public static final int		heartbeat_time = 500;
	
	public static final String	update_file_string = "UPDATEFILE!";
	
	public static final String 	set_primary_string = "PRIMARY_UPDATE_TIME";
	
	public static final String 	suspect_string = "SUSPECT";
	
	public static final String	ping = "PING";
	public static final String	ack = "ACK";
	
	public static final String	update_rs_list_string = "RSUPDATE";
	
	public static final String 	election_string = "ELECTION";
	
	public static final String	primary_confirm_string = "IMPRIMARY";
	
	public static final String	you_died_string = "YOUJUSTREVIVED";
	
	public static final String  revival_file_update_string = "GIVEMEUPDATES";
	
	public static final String 	update_rm_list_string = "UPDATE RM ";
	
	public static final String 	request_file_info = "GIVEMEFILES";
	
	public static final String 	request_file_updates = "GIVEMEUPDATESON";
	
	public static final String  pause_heartbeat = "PAUSEHB!";
}
