package cl.cam.ac.uk.tuneful;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;

import cl.cam.ac.uk.tuneful.util.TunefulFactory;

public class Tuneful {
    public static void main( String[] args )
    {
        Hashtable<String, String> conf;
        String appName = "ScalaWordCount"; // TODO: take from stdin

		// set the values in the sparkConf
		if (!TunefulFactory.getSignificanceAnalyzer().isSigParamDetected(appName)) {
			System.out.println(">>> Sig param are not detected yet ...");
			// get the conf using SA
			conf = TunefulFactory.getSignificanceAnalyzer().suggestNextConf(appName);
		} else {
			// get the conf using cost modeler
			System.out.println(">>> Sig param detected ... get conf using cost modeler");
			conf = TunefulFactory.getCostModeler().findCandidateConf(appName);
		}

        try {
            String samplesFileName = TunefulFactory.getTunefulHome() + "/spconfig";
            FileWriter fileWriter = new FileWriter(samplesFileName , false);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

            for (String key : conf.keySet()) {
                System.out.println(">>> " + key + " >>> " + conf.get(key));
                bufferedWriter.write("--conf " + key + "=" + conf.get(key) + " ");
            }
			bufferedWriter.flush();
			bufferedWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
}
