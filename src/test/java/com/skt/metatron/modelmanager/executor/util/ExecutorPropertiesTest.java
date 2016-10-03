package com.skt.metatron.modelmanager.executor.util;

import com.skt.metatron.modelmanager.executor.ModelManagerExecutor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Created by kyungtaak on 2016. 10. 3..
 */
public class ExecutorPropertiesTest {

  @Test
  public void extractKeyValues() {
    String test1 = "--function.className=abc.def.MainClass";
    System.out.println(new ExecutorProperties(new String[] {"a"}).extractKeyValues(test1));

    String test2 = "--function.args.json={\"abc\" : \"xyz\",\"cde\" : 0.5 \n, \"nested\": [[0.0, 0.0],[0.0, 0.0]], \"strarray\":[\"a\", \"b\"]}";
    System.out.println(new ExecutorProperties(new String[] {"a"}).extractKeyValues(test2));
  }

  @Test
  public void uplaodTest() throws IOException {
    String path = "/Users/kyungtaak/IdeaProjects/metatron-modelmanager-executor/target/metatron-modelmanager-executor-1.0.0-SNAPSHOT-jar-with-dependencies.jar";

    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://emn-g04-03:9000");

    System.out.println(ModelManagerExecutor.processDataFile(path, FileSystem.get(conf)));

  }

}