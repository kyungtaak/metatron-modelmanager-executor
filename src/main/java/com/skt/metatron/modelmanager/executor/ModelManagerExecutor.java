package com.skt.metatron.modelmanager.executor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.skt.metatron.modelmanager.executor.exception.ExecutorException;
import com.skt.metatron.modelmanager.executor.util.ExecutorProperties;
import com.skt.metatron.modelmanager.executor.util.SerializableStringJoiner;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by kyungtaak on 2016. 10. 3..
 */
public class ModelManagerExecutor {

  static {
    System.setProperty("logback.configurationFile", "logback-executor-console.xml");
  }

  private static Logger LOGGER = LoggerFactory.getLogger(ModelManagerExecutor.class);

  public static void main(String[] args) {
    System.out.println("Envs ===================================== ");
    System.out.println(System.getenv());

    System.out.println("Args ==================================== ");
    System.out.println(Arrays.toString(args));

    try {
      ExecutorProperties properties = new ExecutorProperties(args);

      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          System.out.println("Shutdown hook ran.");
        }
      });


      // 1. Create SparkContext
      SparkContext sparkContext = new SparkContext(properties.getSparkConf());
      String binaryPath = properties.getBinaryPath();
      if(StringUtils.isNotEmpty(binaryPath)) {
        sparkContext.addJar(binaryPath);
      }

      String checkpointDir = properties.getCheckPointDir();
      if(StringUtils.isNotEmpty(checkpointDir)) {
        sparkContext.setCheckpointDir(checkpointDir);
      }

      SQLContext sqlContext = new SQLContext(sparkContext);
      LOGGER.info("Successfully created sparkcontext.");

      FileSystem hdfs = null;
      if(properties.getMaster().indexOf("yarn") > -1) {
        hdfs = FileSystem.get(properties.getHadoopConf());
      }

      // 2. Load TargetDataFrames
      List<String> paths = properties.getDataPaths();
      List<DataFrame> mainDataFrames = new ArrayList<>();

      ObjectMapper mapper = new ObjectMapper();
      for (String path : paths) {
        File original = new File(path);
        JsonNode jsonNode = mapper.readValue(original, JsonNode.class);
        File newFile = new File(original.getAbsolutePath() + "_new");

        FileWriter fw = new FileWriter(newFile);
        for(JsonNode node : jsonNode) {
          fw.write(mapper.writeValueAsString(node) + "\n");
        }
        fw.close();

        String filePath = processDataFile(newFile.getAbsolutePath(), hdfs);
        LOGGER.info("Target Data File : {}", filePath);
        DataFrame df = sqlContext.read().json(filePath);
        System.out.println("Main-----");
        df.printSchema();
        //df = df.select("chamberId", "lotId", "paramName", "procId", "recipeId", "stepId", "timestamp", "waferId").where("_corrupt_record != '[' and _corrupt_record != ']");
        mainDataFrames.add(df);
      }
      LOGGER.info("Successfully load main dataframe.");

      // 3. Load ModelDataFrames
      String modelPath = properties.getModelPath();
      DataFrame modelDataFrame;
      if (StringUtils.isNotEmpty(modelPath)) {
        modelDataFrame = sqlContext.read().json(properties.getModelPath());
      } else {
        modelDataFrame = sqlContext.emptyDataFrame();
      }
      LOGGER.info("Successfully load model dataframe");

      // 2. Run Invoke
      DataFrame result = invokeJob(sparkContext,
              properties,
              mainDataFrames.toArray(new DataFrame[mainDataFrames.size()]),
              modelDataFrame,
              properties.getArgs()
      );

      if(!sparkContext.isStopped()){
        createOutputFile(result, properties.getOutputFormat(), properties.getOutputPath());
        LOGGER.info("Successfully create output");
      }else
        LOGGER.error("Failure - You must not stop SparkContext");
    } catch (Exception e) {
      LOGGER.error("Executor Error: {}", e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }

  }

  public static String processDataFile(String path, FileSystem hdfs) {

    LOGGER.info("HDFS object : " + hdfs);
    if(hdfs == null) {
      return path;
    }

    URI uri = URI.create(path);
    if("hdfs".equals(uri.getScheme())) {
      return path;
    }

    System.out.println("path : " + path);
    System.out.println("URI : " + uri.toString());
    Path localPath = new Path(path);
    Path hdfsPath = new Path("/tmp/" + localPath.getName());

    try {
      hdfs.copyFromLocalFile(true, localPath, hdfsPath);
    } catch (IOException e) {
      throw new ExecutorException("Fail to copy to HDFS : " + e.getMessage());
    }
    System.out.println("HDFS URI : " + hdfsPath.toUri());

    return "hdfs://" + hdfsPath.toUri().toString();
  }

  /**
   * 수행함수 정보를 토대로 Invoke 수행 (Reflection)
   */
  public static DataFrame invokeJob(SparkContext sparkContext, ExecutorProperties properties, DataFrame[] inputData, DataFrame modelData, Object arguments) {

    Class<?> clazz;
    try {
      clazz = ClassUtils.getClass(properties.getClassName());
    } catch (ClassNotFoundException e) {
      LOGGER.error("Fail to load class : {}", e.getMessage());
      throw new ExecutorException("Class load error : " + e.getMessage());
    }

    LOGGER.debug("Get class in classloader : {}", clazz);

    // Reflect class object
    //
    Object serviceObj = null;
    try {
      serviceObj = ConstructorUtils.invokeConstructor(clazz);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
      LOGGER.error("Fail to initialize class : {}", e.getMessage());
      throw new ExecutorException("Fail to initialize class : " + e.getMessage());
    }

    LOGGER.debug("Reflect class object : {}", serviceObj);

    // InvokeArgType 에 따라 메소드 호출에 필요한 Arguments 셋팅(Custom Argment 제외)
    //
    List<Object> invokeArgs = makeArguments(sparkContext, properties, inputData, modelData, arguments);

    // Invoke Method and Get result dataframe
    //
    DataFrame resultDataFrame;
    try {
      // List 형식의 데이터를 가변인자로 전환시 toArray 메소드 적용, Size 지정 필수
      Object result = MethodUtils.invokeMethod(serviceObj, properties.getMethodName()
              , invokeArgs.toArray(new Object[invokeArgs.size()]));
      resultDataFrame = (result == null) ? null : (DataFrame) result;
    } catch (NoSuchMethodException | IllegalAccessException e) {
      LOGGER.error("Fail to invoke method({}) : {}", properties.getMethodName(), e.getMessage());
      throw new ExecutorException("Fail to invoke method : " + e.getMessage());
    } catch (InvocationTargetException e) {
      Throwable t = e.getCause();
      if (t == null) {
        t = e.getTargetException();
      }
      LOGGER.error("Fail to run method({}) : {}", properties.getMethodName(), t.getMessage());
      throw new ExecutorException(t.getMessage());
    }

    LOGGER.info("Sucessfully invoke analysis method.");

    return resultDataFrame;
  }

  public static List<Object> makeArguments(SparkContext sparkContext, ExecutorProperties properties, DataFrame[] inputData, DataFrame modelData, Object arguments) {

    String argType = properties.getArgType();
    String invokeType = properties.getInvokeType();
    Object args = properties.getArgs();

    if (arguments != null) {

      switch (argType) {
        case "ARRAY":
          if (!(arguments instanceof List)) {
            throw new ExecutorException("It does not match the pre-defined type(" + argType + ") of argument.");
          }

          List<Object> tempArgsList = (List) arguments;

          arguments = tempArgsList.toArray(new Object[tempArgsList.size()]);
          break;

        case "MAP":
          if (!(arguments instanceof Map)) {
            throw new ExecutorException("It does not match the pre-defined type(" + argType + ") of argument.");
          }
      }
    }

    List<Object> invokeArgs = null;
    switch (invokeType) {
      case "CUSTOM1":
        invokeArgs = Lists.newArrayList(sparkContext, inputData == null || inputData.length == 0 ? null : inputData[0]);
        break;
      case "CUSTOM2":
        invokeArgs = Lists.newArrayList(sparkContext, inputData, modelData);
        break;
      default:
        invokeArgs = Lists.newArrayList(sparkContext, inputData);
    }

    invokeArgs.add(arguments == null ? null : arguments);

    LOGGER.debug("Argument List for Invoke: {}", Arrays.toString(invokeArgs.toArray(new Object[invokeArgs.size()])));

    return invokeArgs;
  }

  public static void createOutputFile(DataFrame output, String outputFormat, String outputPath) {

    Preconditions.checkArgument(StringUtils.isNotEmpty(outputFormat), "outputFormat must be not null.");

    List<Column> columns = Lists.newArrayList();
    String[] projections = StringUtils.split(outputFormat, ",");
    for (String projection : projections) {
      columns.add(new Column(projection));
    }

    LOGGER.debug("Output column is {}", Arrays.toString(projections));

    SerializableStringJoiner joiner = new SerializableStringJoiner(",", "[", "]");

    try {

      System.out.println("Selected Result -----");
      DataFrame resultDf = output.select(columns.toArray(new Column[columns.size()]));
      resultDf.printSchema();
      //resultDf.show();

      JavaRDD<String> resultRdd = resultDf.toJSON().toJavaRDD();

      if (resultRdd != null) {
        List<String> rdds = resultRdd.collect();
        for (String rdd : rdds) {
          joiner.add(rdd);
        }
      }
    } catch (Exception e) {
      // 지정한 칼럼과 결과 데이터 프레임의 결과가 맞지 않는 경우 발생 Case.
      LOGGER.error("Fail to get output from DataFrame : {}", e.getMessage());
      throw new ExecutorException("Fail to get output from DataFrame : " + e.getMessage());
    }

    try (PrintWriter out = new PrintWriter(outputPath)) {
      out.println(joiner.toString());
    } catch (FileNotFoundException e) {
      throw new ExecutorException("Fail to write output file : " + e.getMessage());
    }
  }
}
