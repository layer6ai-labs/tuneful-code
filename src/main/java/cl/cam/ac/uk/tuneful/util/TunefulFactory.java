package cl.cam.ac.uk.tuneful.util;

import java.io.File;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;

import cl.cam.ac.uk.tuneful.ConfParam;
import cl.cam.ac.uk.tuneful.ConfigurationSampler;
import cl.cam.ac.uk.tuneful.ConfigurationTuner;
import cl.cam.ac.uk.tuneful.CostModeler;
import cl.cam.ac.uk.tuneful.SignificanceAnalyzer;
import cl.cam.ac.uk.tuneful.wl.characterization.WorkloadCharacterizer;
import cl.cam.ac.uk.tuneful.wl.characterization.WorkloadMonitor;

public class TunefulFactory {
	static ConfigurationTuner tuner = null;
	static WorkloadCharacterizer characterizer = null;
	static WorkloadMonitor monitor = null;
	static ConfigurationSampler configurationSampler = null;
	static SignificanceAnalyzer significanceAnalyzer = null;
	static HttpConnector connector = new HttpConnector();
	static Hashtable<String, Integer>n_executions = null;// number of WL executions
	static String n_executionsPath = TunefulFactory.getTunefulHome() + "/n_executions.ser";

	public static HttpConnector getHttpConnector() {
		return connector;
	}

	public static ConfigurationTuner getConfigurationTuner() {
		if (tuner == null) {
			tuner = new ConfigurationTuner();

		}
		return tuner;
	}

	public static WorkloadCharacterizer getWorkloadCharacterizer() {
		if (characterizer == null) {
			characterizer = new WorkloadCharacterizer();

		}
		return characterizer;
	}

	public static WorkloadMonitor getWorkloadMonitor() {
		if (monitor == null) {
			monitor = new WorkloadMonitor();

		}
		return monitor;
	}

	public static CostModeler getCostModeler() {
		return new CostModeler();
	}

	public static List<String> getTunableParams() {
		// TODO read conf params from properties file, have default tunable conf

		String[] conf = {
			"spark.executor.cores",
			"spark.executor.memory",
			"spark.executor.instances",
			"spark.default.parallelism",
			"spark.memory.offHeap.enabled",
			
			"spark.memory.offHeap.size",
			"spark.memory.fraction",
			"spark.memory.storageFraction",
			"spark.shuffle.file.buffer",
			"spark.speculation",
		
			"spark.reducer.maxSizeInFlight",
			"spark.shuffle.sort.bypassMergeThreshold",
			"spark.speculation.interval",
			"spark.speculation.multiplier",
			"spark.speculation.quantile",

			"spark.broadcast.blockSize",
			"spark.io.compression.codec",
			"spark.io.compression.lz4.blockSize",
			"spark.io.compression.snappy.blockSize",
			"spark.kryo.referenceTracking",

			"spark.kryoserializer.buffer.max",
			"spark.kryoserializer.buffer",
			"spark.storage.memoryMapThreshold",
			// "spark.network.timeout",
			"spark.locality.wait",
	
			"spark.shuffle.compress",
			"spark.shuffle.spill.compress",
			"spark.broadcast.compress",
			"spark.rdd.compress",
			"spark.serializer",
		};
		return Arrays.asList(conf);
	}

	public static Hashtable<String, ConfParam> getTunableParamsRange() {

		// TODO read conf params from properties file, have defaulttunable params ranges
		Hashtable<String, ConfParam> paramsRange = new Hashtable<String, ConfParam>();
		paramsRange.put("spark.executor.cores",
			new ConfParam("spark.executor.cores", "int", new double[] { 1.0, 16.0 }, new String[] { "" }, ""));
		paramsRange.put("spark.executor.memory",
			new ConfParam("spark.executor.memory", "int", new double[] { 5.0, 43.0 }, new String[] { "" }, "g"));
		paramsRange.put("spark.executor.instances",
			new ConfParam("spark.executor.instances", "int", new double[] { 8.0, 48.0 }, new String[] { "" }, ""));
		paramsRange.put("spark.default.parallelism",
			new ConfParam("spark.default.parallelism", "int", new double[] { 8.0, 50.0 }, new String[] { "" }, ""));
		paramsRange.put("spark.memory.offHeap.enabled",
			new ConfParam("spark.memory.offHeap.enabled", "boolean", new double[] { 0.0, 0.0 }, new String[] { "true", "false"  }, ""));

		paramsRange.put("spark.memory.offHeap.size",
			new ConfParam("spark.memory.offHeap.size", "int", new double[] { 10.0, 100.0 }, new String[] { "" }, "m"));
		paramsRange.put("spark.memory.fraction",
			new ConfParam("spark.memory.fraction", "float", new double[] { 0.5, 1 }, new String[] { "" }, ""));
		paramsRange.put("spark.memory.storageFraction",
			new ConfParam("spark.memory.storageFraction", "float", new double[] { 0.5, 1 }, new String[] { "" }, ""));
		paramsRange.put("spark.shuffle.file.buffer",
			new ConfParam("spark.shuffle.file.buffer", "int", new double[] { 2.0, 128.0 }, new String[] { "" }, "k"));
		paramsRange.put("spark.speculation",
			new ConfParam("spark.speculation", "boolean", new double[] { 0.0, 0.0 }, new String[] { "true", "false"  }, ""));

		paramsRange.put("spark.reducer.maxSizeInFlight",
			new ConfParam("spark.reducer.maxSizeInFlight", "int", new double[] { 2.0, 128.0 }, new String[] { "" }, "m"));
		paramsRange.put("spark.shuffle.sort.bypassMergeThreshold",
			new ConfParam("spark.shuffle.sort.bypassMergeThreshold", "int", new double[] { 100.0, 1000.0 }, new String[] { "" }, ""));
		paramsRange.put("spark.speculation.interval",
			new ConfParam("spark.speculation.interval", "int", new double[] { 10.0, 100.0 }, new String[] { "" }, "ms"));
		paramsRange.put("spark.speculation.multiplier",
			new ConfParam("spark.speculation.multiplier", "float", new double[] { 1.0, 5.0 }, new String[] { "" }, ""));
		paramsRange.put("spark.speculation.quantile",
			new ConfParam("spark.speculation.quantile", "float", new double[] { 0.0, 1.0 }, new String[] { "" }, ""));

		paramsRange.put("spark.broadcast.blockSize",
			new ConfParam("spark.broadcast.blockSize", "int", new double[] { 2.0, 128.0 }, new String[] { "" }, "m"));
		paramsRange.put("spark.io.compression.codec", new ConfParam("spark.io.compression.codec", "enum", new double[] { 0.0, 0.0 },
			new String[] { "snappy", "lzf", "lz4" }, ""));
		paramsRange.put("spark.io.compression.lz4.blockSize",
			new ConfParam("spark.io.compression.lz4.blockSize", "int", new double[] { 2.0, 32.0 }, new String[] { "" }, "m"));
		paramsRange.put("spark.io.compression.snappy.blockSize",
			new ConfParam("spark.io.compression.snappy.blockSize", "int", new double[] { 2.0, 128.0 }, new String[] { "" }, "m"));
		paramsRange.put("spark.kryo.referenceTracking", new ConfParam("spark.kryo.referenceTracking", "boolean", new double[] { 0.0, 0.0 },
				new String[] { "true", "false" }, ""));

		paramsRange.put("spark.kryoserializer.buffer.max",
			new ConfParam("spark.kryoserializer.buffer.max", "int", new double[] { 8.0, 128.0 }, new String[] { "" }, "m"));
		paramsRange.put("spark.kryoserializer.buffer",
			new ConfParam("spark.kryoserializer.buffer", "int", new double[] { 2.0, 128.0 }, new String[] { "" }, ""));
		paramsRange.put("spark.storage.memoryMapThreshold",
			new ConfParam("spark.storage.memoryMapThreshold", "int", new double[] { 50.0, 500.0 }, new String[] { "" }, ""));
		// paramsRange.put("spark.network.timeout",
		// 	new ConfParam("spark.network.timeout", "int", new double[] { 20.0, 500.0 }, new String[] { "" }, "s"));
		paramsRange.put("spark.locality.wait",
			new ConfParam("spark.locality.wait", "int", new double[] { 1.0, 10.0 }, new String[] { "" }, ""));

		paramsRange.put("spark.shuffle.compress", new ConfParam("spark.shuffle.compress", "boolean", new double[] { 0.0, 0.0 },
			new String[] { "true", "false" }, ""));
		paramsRange.put("spark.shuffle.spill.compress", new ConfParam("spark.shuffle.spill.compress", "boolean", new double[] { 0.0, 0.0 },
			new String[] { "true", "false" }, ""));
		paramsRange.put("spark.broadcast.compress", new ConfParam("spark.broadcast.compress", "boolean", new double[] { 0.0, 0.0 },
			new String[] { "true", "false" }, ""));
		paramsRange.put("spark.rdd.compress", new ConfParam("spark.rdd.compress", "boolean", new double[] { 0.0, 0.0 },
				new String[] { "true", "false" }, ""));
		paramsRange.put("spark.serializer",
				new ConfParam("spark.serializer", "enum", new double[] { 0.0, 0.0 }, new String[] {
						"org.apache.spark.serializer.JavaSerializer", "org.apache.spark.serializer.KryoSerializer" },
						""));
		return paramsRange;
	}

	public static ConfigurationSampler getConfigurationSampler() {
		if (configurationSampler == null) {
			configurationSampler = new ConfigurationSampler();

		}
		return configurationSampler;
	}

	public static SignificanceAnalyzer getSignificanceAnalyzer() {
		if (significanceAnalyzer == null) {
			significanceAnalyzer = new SignificanceAnalyzer();
			System.out.println("SIg Analyzer created ...");

		}
		return significanceAnalyzer;

	}

	public static String getSamplesFilePath(String appName, Integer integer) {

		final String BASE = "execution_samples_";
		return getTunefulHome() + "/" + BASE + appName + "_SA_" + integer;
	}

	public static Object getSigParamsFileName(String appName, Integer integer) {
		final String BASE = "sig_params_";
		return BASE + appName + "_SA_" + integer;
	}

	public static String getTunefulHome() {
		String TUNEFUL_HOME =  System.getenv("HOME") + "/tuneful";
		File directory = new File(TUNEFUL_HOME);
		if (!directory.exists()) {
			directory.mkdirs();
		}
		return TUNEFUL_HOME;
	}

	public static Hashtable<String , Integer> get_n_executions() {
		if (n_executions == null) {
			n_executions = new Hashtable<String, Integer>();
			n_executions = Util.loadTable(n_executionsPath);
		}
		return n_executions;
	}

	public void set_n_executions(Hashtable n_executions) {
		this.n_executions = n_executions;
	}

	public static String getAppExecTimeFilePath(String appName) {
		// TODO Auto-generated method stub
		return getTunefulHome() + "/" +appName+"conf_exec_time.csv";
	}

	public static String getExecutionsPath() {
		return getTunefulHome() +  "/n_executions.ser";
	}

}
