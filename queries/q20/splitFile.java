import java.io.*;
import java.lang.IllegalArgumentException;


public class splitFile {
	public static void main (String[] args) {
		
		String path = System.getProperty("user.dir");
		// System.out.println(path);

		int length = args.length;

		if ( length != 1 ) {
			throw new IllegalArgumentException("SplitFile program takes only 1 argument!");
		} else {
		
			try {
				String targetDir = args[0];
				File inputFile = new File(targetDir + "/000000_0");
				FileInputStream fs = new FileInputStream(inputFile);
				InputStreamReader isr = new InputStreamReader(fs);
				BufferedReader br = new BufferedReader(isr);
			
				String line = "";
				// System.out.println(inputFile.getAbsolutePath());
				while ( (line=br.readLine()) != null ) {
					String[] items = line.split(" ");
					String name = items[0];
					String filePath = targetDir + "/input/" + name;
					File customer = new File(filePath);
					if ( !customer.exists() ) {
						FileWriter fw = new FileWriter(customer.getAbsoluteFile());
						BufferedWriter bw = new BufferedWriter(fw);
						bw.write(line);
						bw.close();
					}
				}
			inputFile.delete();
			} catch (IOException e) {
				System.out.println("There was an error reading/writing the specified file!");
			}
		
			System.out.println("SPLITTING FILE -- SUCCESS.");
		}
	}
}
