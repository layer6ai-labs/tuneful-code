package cl.cam.ac.uk.tuneful;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;

import cl.cam.ac.uk.tuneful.util.TunefulFactory;

public class CostModeler {

	private String modelPath = null;
	private String MODEL_INPUT_PATH_BASE;

	public CostModeler() {
		try {
			MODEL_INPUT_PATH_BASE = this.getClass().getClassLoader().getResource("\\home\\tuneful\\spearmint-lite\\")
					.toURI().getPath();
			File directory = new File(MODEL_INPUT_PATH_BASE);
			if (!directory.exists()) {
				directory.mkdirs();
			}
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}

	}

	// write the config Json file for Spearmint to start the GP optimisation
	public void writeParamsJsonFile(String appName) {
		// write each param name, type, min, max in the app dir
		String filePath = MODEL_INPUT_PATH_BASE + appName + "\\config.json"; // TODO: add line to check if dir does not
																				// exist then create
		List<String> confParams = TunefulFactory.getSignificanceAnalyzer().getSignificantParams(appName);
		for (int i = 0; i < confParams.size(); i++) {
			ConfParam currentParam = TunefulFactory.getSignificanceAnalyzer().getAllParams().get(confParams.get(i));
			writeParamToFile(currentParam, filePath);

		}
	}

	private void writeParamToFile(ConfParam currentParam, String filePath) {
		FileWriter fileWriter;
		try {
			String appModelInputPath = filePath;
			fileWriter = new FileWriter(appModelInputPath);
			BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
			bufferedWriter.write(currentParam.toJsonString());
			bufferedWriter.flush();
			bufferedWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public SparkConf findCandidateConf(SparkConf conf, String appName) {
		// generate pending conf using speamint
		runSpearmint(appName);
		Hashtable<String, String> tunedConf = readPendingConf(appName);
		SparkConf updatedConf = conf;
		Set<String> keys = tunedConf.keySet();
		for (String key : keys) {
			updatedConf.set(key, tunedConf.get(key));
		}

		return updatedConf;
	}


	private void writeToModelInput(Hashtable conf, double cost, String appName) {
		FileWriter fileWriter;
		try {
			String appModelInputPath = MODEL_INPUT_PATH_BASE + appName + "\\result.dat";
			fileWriter = new FileWriter(appModelInputPath);
			BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
			List<String> confParams = TunefulFactory.getSignificanceAnalyzer().getSignificantParams(appName);
			String line = cost + " 0 " + getConfAsstr(conf, confParams) + " \\n";
			bufferedWriter.write(line);
			bufferedWriter.flush();
			bufferedWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private String getConfAsstr(Hashtable conf, List<String> confParams) {
		String confAsStr = "";
		for (int i = 0; i < confParams.size(); i++) {
			confAsStr += conf.get(confParams.get(i)) + " ";
		}
		return confAsStr;
	}

	private void writePendingConf(Hashtable conf, String appName) {
		FileWriter fileWriter;
		try {
			String appModelInputPath = MODEL_INPUT_PATH_BASE + appName + "\\result.dat";
			fileWriter = new FileWriter(appModelInputPath);
			BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
			List<String> confParams = TunefulFactory.getSignificanceAnalyzer().getSignificantParams(appName);
			String line = "P P " + getConfAsstr(conf, confParams) + " \\n";
			bufferedWriter.write(line);
			bufferedWriter.flush();
			bufferedWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private Hashtable<String, String> readPendingConf(String appName) {
		Hashtable<String, String> conf = new Hashtable<String, String>();
		try {
			String appModelDir = MODEL_INPUT_PATH_BASE + appName + "\\result.dat";
			FileReader fileReader = new FileReader(appModelDir);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line;
			line = bufferedReader.readLine();
			String[] splittedLine = line.split(" ");
			List<String> confParams = TunefulFactory.getSignificanceAnalyzer().getSignificantParams(appName);
			while (line != null) {
				if (splittedLine.length == confParams.size() + 2) // number of elements per line
				{
					if (line.contains("P")) {
						// parse line into conf and add to the hashtable
						conf = parseLine(line, confParams, appName);

					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return conf;
	}

	private Hashtable<String, String> parseLine(String line, List<String> confParams, String appName) {

		Hashtable<String, String> conf = new Hashtable<String, String>();
		String[] splittedLine = line.split(" ");
		for (int i = 2; i < confParams.size(); i++) {
			conf.put(confParams.get(i - 2), splittedLine[i]);
		}
		return conf;
	}

//	private Hashtable<String, String> getTunableParams(SparkConf sparkconf, String appName) {
//		Hashtable<String, String> tunableParams = new Hashtable<String, String>();
//		List<String> confParams = TunefulFactory.getSignificanceAnalyzer().getSignificantParams(appName);
//		for (int i = 0; i < confParams.size(); i++) {
//			tunableParams.put(confParams.get(i), sparkconf.get(confParams.get(i)));
//		}
//		return tunableParams;
//	}

	public void updateModel(String appName, Hashtable<String, String> conf, double cost) {

		writeToModelInput(conf, cost, appName);
		runSpearmint(appName);

	}

	public String getModelPath() {
		return modelPath;
	}

	public void setModelPath(String modelPath) {
		this.modelPath = modelPath;
	}

	private void runSpearmint(String appName) {

		try {
			String appModelDir = MODEL_INPUT_PATH_BASE + appName;
			final Map<String, String> envMap = new HashMap<String, String>(System.getenv());
			String pythonHome = envMap.get("PYTHON_HOME");
			File file = new File(this.getClass().getClassLoader().getResource("spearmint-lite\\spearmint-lite.py")
					.toURI().getPath());

			String pythonFile = file.getAbsolutePath();
			System.out.println("file path >> " + pythonFile);

			ProcessBuilder pb = new ProcessBuilder(pythonHome + "\\python ", pythonFile, "--driver=local",
					"--method=GPEIOptChooser", "-method-args=noiseless=1", appModelDir);
			pb.redirectError();
			Process p = pb.start();

			InputStream is = null;
			try {
				is = p.getInputStream();
				int in = -1;
				while ((in = is.read()) != -1) {
					System.out.print((char) in);
				}
			} finally {
				try {
					is.close();
				} catch (Exception e) {
				}
			}

			System.out.println("command executed ! ");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		try {

			final Map<String, String> envMap = new HashMap<String, String>(System.getenv());
			String pythonHome = envMap.get("PYTHON_HOME");
			File file = new File(new CostModeler().getClass().getClassLoader()
					.getResource("spearmint-lite\\spearmint-lite.py").toURI().getPath());
//			File file = new File (new CostModeler().getClass().getClassLoader()
//					.getResource("test.py").toURI()
//					.getPath());

			String pythonFile = file.getAbsolutePath();
			System.out.println("file path >> " + pythonFile);
//			Runtime.getRuntime().exec(new String[] {pythonHome+"\\python " , pythonFile } );

			ProcessBuilder pb = new ProcessBuilder(pythonHome + "\\python ", pythonFile);
			pb.redirectError();
			Process p = pb.start();

			InputStream is = null;
			try {
				is = p.getInputStream();
				int in = -1;
				while ((in = is.read()) != -1) {
					System.out.print((char) in);
				}
			} finally {
				try {
					is.close();
				} catch (Exception e) {
				}
			}

			System.out.println("command executed ! ");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}