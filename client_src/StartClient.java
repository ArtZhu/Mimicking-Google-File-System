import java.io.IOException;
import java.util.Scanner;


public class StartClient {
	
	public static void main(String[] args) throws IOException{
		FileSystemClient c = new Client_v1();
		
		Scanner sc = new Scanner(System.in);
		System.out.println("New command");
		while(sc.hasNextLine()){
			System.out.println("New command");
			String s = sc.nextLine();
			int i = s.indexOf(" ");
			if(s.substring(0, i).equals("Upload")){
				c.uploadFile(s.substring(i+1));
			}else
				c.downloadFile(s.substring(i+1));
		}
	}
}
