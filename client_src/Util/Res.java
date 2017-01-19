package Util;

import java.util.regex.Pattern;

public class Res {
	public static final String 	server_string 	= "serverIP";
	public static final String 	EOF				= "EOF!!!";
	
	public static final String	separator 	= ":";
	
	public static final String 	upload_string 	= "upload file";
	public static final String 	download_string 	= "download file";
	public static final String 	file_loc_request_string = "Request Location of";
	
	public static final String 	ResS_RM_join_as_server_request = "Requesting to join as server";
	public static final String 	ResS_RM_join_as_server_confirm = "You are a server now!";
	public static final String 	ResS_RM_join_as_server_reject = "You failed to be a server!";
	
	public static final String 	RM_ResS_open_TCP_request_string = "Open a TCP!";
	
	public static final int		buf_len			= 1024;
	
	public static final String	log_file_name 	= "log";
	
	public static final String 	file_path 		= "testfiles/";
	
	//Apr 4th, 2016
	public static final String 	RM_FE_register_string = "RMishere";
	
	public static final String	file_loc_found_string = "fileFound";

	public static final String	FE_string = "FrontEndServer";
	
	public static final Pattern addr_port_pattern = Pattern.compile("\\d+\\.\\d\\.\\d+\\.\\d+\\:\\d+");
	
	public static final String 	udp_port_string = "udpport";
	public static final String	mc_port_string = "mcport";
	
	public static final String	ResS_client_TCP_string = "connectTCP";

}
