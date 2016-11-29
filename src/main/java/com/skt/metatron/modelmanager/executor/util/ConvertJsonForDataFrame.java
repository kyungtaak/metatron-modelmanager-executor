package com.skt.metatron.modelmanager.executor.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * Created by kyungtaak on 2016. 11. 29..
 */
public class ConvertJsonForDataFrame {

  public static String convert(String path) throws IOException {

    ObjectMapper mapper = new ObjectMapper();
    String newPath = path + "_new";

    JsonFactory factory = mapper.getFactory();
    JsonParser parser = factory.createParser(new File(path));

    if(parser.nextToken() != JsonToken.START_ARRAY) {
      throw new IllegalStateException("Expected an array");
    }

    BufferedWriter bw = new BufferedWriter(new FileWriter(newPath, true));
    while(parser.nextToken() == JsonToken.START_OBJECT) {
      // read everything from this START_OBJECT to the matching END_OBJECT
      bw.write(mapper.writeValueAsString(parser.readValueAs(Map.class)));
      bw.newLine();
    }

    parser.close();
    bw.close();

    return newPath;
  }
}
