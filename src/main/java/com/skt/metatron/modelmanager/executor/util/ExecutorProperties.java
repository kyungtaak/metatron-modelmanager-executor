package com.skt.metatron.modelmanager.executor.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.skt.metatron.modelmanager.executor.exception.ExecutorException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by kyungtaak on 2016. 10. 3..
 */
public class ExecutorProperties {

  public static final Pattern PATTERN_KEY_VALUE_ARGS = Pattern.compile("^--([^=]+)=(.*)$", Pattern.DOTALL);

  Map<String, String> functionProp = new HashMap<>();

  Object argsProp;

  Map<String, String> sparkProp = new HashMap<>();

  public ExecutorProperties(String[] args) {
    parseArgs(args);
  }

  public SparkConf getSparkConf() {
    System.out.println("############:" + sparkProp.get("spark.appName"));
    SparkConf conf = new SparkConf()
                  .setAppName(sparkProp.get("spark.appName"))
                  .setMaster(sparkProp.get("spark.master"));

    for(String key : sparkProp.keySet()) {
      if(key.indexOf("spark.properties.") == 0) {
        conf.set(StringUtils.substringAfter(key, "spark.properties."), sparkProp.get(key));
      }
    }

    return conf;
  }

  public Configuration getHadoopConf() {
    Configuration conf = new Configuration();

    if(StringUtils.isNotEmpty(conf.get("fs.defaultFS"))) {
      conf.set("fs.defaultFS", sparkProp.get("spark.properties.spark.hadoop.fs.defaultFS"));
    }

    return conf;
  }

  public List<String> getDataPaths() {
    String pathString = functionProp.get("function.dataPath");

    if(StringUtils.isEmpty(pathString)) {
      return new ArrayList<>();
    }

    return Arrays.asList(StringUtils.split(pathString, ","));
  }

  public String getModelPath() {
    return functionProp.get("function.modelPath");
  }

  public String getClassName() {
    return functionProp.get("function.className");
  }

  public String getMethodName() {
    return functionProp.get("function.methodName");
  }

  public String getArgType() {
    return functionProp.get("function.argType");
  }

  public String getInvokeType() {
    return functionProp.get("function.invokeType");
  }

  public String getDataType() {
    return functionProp.get("function.dataType");
  }

  public String getOutputFormat() {
    return functionProp.get("function.outputFormat");
  }

  public String getOutputPath() {
    return functionProp.get("function.outputPath");
  }

  public String getBinaryPath() {
    return functionProp.get("function.binaryPath");
  }

  public String getWorkingDir() {
    return functionProp.get("function.workingDir");
  }

  public String getCheckPointDir() {
    return sparkProp.get("spark.checkpointDir");
  }

  public String getMaster() {
    return sparkProp.get("spark.master");
  }

  public Object getArgs() {
    return argsProp;
  }

  public void parseArgs(String[] args) {

    for(String arg : args) {
      List<String> keyValue = extractKeyValues(arg);
      if(keyValue.size() < 1) {
        continue;
      }
      String key = keyValue.get(0);

      if(key.indexOf("function.args") == 0) {
        try {
          argsProp = new ObjectMapper().readValue(keyValue.get(1), Object.class);
        } catch (IOException e) {
          throw new ExecutorException("Arg parse error");
        }
      } else if(key.indexOf("function.") == 0) {
        functionProp.put(key, keyValue.get(1));
      } else if(key.indexOf("spark.") == 0) {
        sparkProp.put(key, keyValue.get(1));
      }
    }

  }

  public List<String> extractKeyValues(String arg) {

    List<String> groups = new ArrayList<>();

    Matcher m = PATTERN_KEY_VALUE_ARGS.matcher(arg);

    if(m.find()) {
     groups.add(m.group(1));
     groups.add(m.group(2));
    }

    return groups;
  }





}
