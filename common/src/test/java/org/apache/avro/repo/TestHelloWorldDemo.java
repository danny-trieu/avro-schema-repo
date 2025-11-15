/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.avro.repo;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

/**
 * Unit tests for {@link HelloWorldDemo}
 */
public class TestHelloWorldDemo {

  @Test
  public void testHelloWorld() {
    String result = HelloWorldDemo.helloWorld();
    Assert.assertEquals("Should return 'Hello, World!'", "Hello, World!", result);
  }

  @Test
  public void testHelloWorldWithName() {
    String result = HelloWorldDemo.helloWorld("Alice");
    Assert.assertEquals("Should return custom greeting", "Hello, Alice!", result);
  }

  @Test
  public void testHelloWorldWithNullName() {
    String result = HelloWorldDemo.helloWorld(null);
    Assert.assertEquals("Should return default greeting for null", "Hello, World!", result);
  }

  @Test
  public void testHelloWorldWithEmptyName() {
    String result = HelloWorldDemo.helloWorld("");
    Assert.assertEquals("Should return default greeting for empty", "Hello, World!", result);
  }

  @Test
  public void testHelloWorldWithWhitespaceName() {
    String result = HelloWorldDemo.helloWorld("   ");
    Assert.assertEquals("Should return default greeting for whitespace", "Hello, World!", result);
  }

  @Test
  public void testGetHelloWorldMessage() {
    String message = HelloWorldDemo.getHelloWorldMessage();
    Assert.assertEquals("Should return message without printing", "Hello, World!", message);
  }

  @Test
  public void testHelloWorldPrintsToConsole() {
    // Capture System.out
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(outContent));

    try {
      HelloWorldDemo.helloWorld();
      String output = outContent.toString().trim();
      Assert.assertEquals("Should print to console", "Hello, World!", output);
    } finally {
      // Restore System.out
      System.setOut(originalOut);
    }
  }

  @Test
  public void testHelloWorldWithNamePrintsToConsole() {
    // Capture System.out
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(outContent));

    try {
      HelloWorldDemo.helloWorld("Test");
      String output = outContent.toString().trim();
      Assert.assertEquals("Should print custom message to console", "Hello, Test!", output);
    } finally {
      // Restore System.out
      System.setOut(originalOut);
    }
  }

  @Test
  public void testMainMethodWithNoArgs() {
    // Capture System.out
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(outContent));

    try {
      HelloWorldDemo.main(new String[]{});
      String output = outContent.toString().trim();
      Assert.assertEquals("Should print default message", "Hello, World!", output);
    } finally {
      // Restore System.out
      System.setOut(originalOut);
    }
  }

  @Test
  public void testMainMethodWithArgs() {
    // Capture System.out
    ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    System.setOut(new PrintStream(outContent));

    try {
      HelloWorldDemo.main(new String[]{"Avro", "Schema", "Repository"});
      String output = outContent.toString().trim();
      Assert.assertEquals("Should print custom message from args",
          "Hello, Avro Schema Repository!", output);
    } finally {
      // Restore System.out
      System.setOut(originalOut);
    }
  }
}
