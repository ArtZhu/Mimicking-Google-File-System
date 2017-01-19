
public interface FileSystemClient{
	static final String client_config_file_name = "ClientConfig";
	
	public void downloadFile(String file_name);
	public void uploadFile(String file_name);
}
