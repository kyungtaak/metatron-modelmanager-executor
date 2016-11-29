package com.skt.metatron.modelmanager.executor.util;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by kyungtaak on 2016. 11. 29..
 */
public class ConvertJsonForDataFrameTest {
  @Test
  public void convert() throws Exception {
    String testPath = "src/test/resources/test.json";

    ConvertJsonForDataFrame.convert(testPath);
  }

}