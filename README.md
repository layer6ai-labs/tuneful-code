# Tuneful

Implementation of https://tfjmp.org/publications/2020-kdd.pdf

## Dependency
Install Python 2.7 on your driver node.
```
pip install numpy pandas scipy scikit-learn
```

## Package Tuneful
```
mvn clean package
```

## Config Parameters
I added tunable parameters as listed in the Tuneful paper. You may want to change parameter ranges depending on your cluster spec here https://github.com/layer6ai-labs/tuneful-code/blob/master/src/main/java/cl/cam/ac/uk/tuneful/util/TunefulFactory.java#L101

## Run with Hibench

In Hibench, replace the following line at https://github.com/Intel-bigdata/HiBench/blob/master/bin/functions/workload_functions.sh#L220
```
    else
        SUBMIT_CMD="${SPARK_HOME}/bin/spark-submit ${LIB_JARS} --properties-file ${SPARK_PROP_CONF} --class ${CLS} --master ${SPARK_MASTER} ${YARN_OPTS} ${SPARKBENCH_JAR} $@"
```
into
```
    else
        java -cp ~/{path-to-tuneful}/target/tuneful-0.0.1-SNAPSHOT-jar-with-dependencies.jar cl.cam.ac.uk.tuneful.Tuneful {app-name}
        LIB_JARS="$LIB_JARS --jars ~/{path-to-tuneful}/target/tuneful-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
        SPARK_CONF=`cat ~/tuneful/spconfig`
        SPARK_CONF="$SPARK_CONF --conf spark.extraListeners=cl.cam.ac.uk.tuneful.TunefulListener"
        SUBMIT_CMD="${SPARK_HOME}/bin/spark-submit ${LIB_JARS} --properties-file ${SPARK_PROP_CONF} ${SPARK_CONF} --class ${CLS} --master ${SPARK_MASTER} ${YARN_OPTS} ${SPARKBENCH_JAR} $@"
```

You need to pass {app-name} to tuneful, for example, "ScalaWordCount".  
After each run (eg,`bin/workloads/micro/wordcount/spark/run.sh`), you can find the spark config parameters and execution time in 
`$HOME/tuneful/{app-name}conf_exec_time.csv`
