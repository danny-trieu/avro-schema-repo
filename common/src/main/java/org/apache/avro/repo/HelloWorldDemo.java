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

/**
 * A simple demonstration class that showcases a basic Hello World example.
 * This class can be used as a standalone demo or for testing purposes.
 */
public class HelloWorldDemo {

  /**
   * Main entry point for the Hello World demonstration.
   *
   * @param args command-line arguments (optional: custom message)
   */
  public static void main(String[] args) {
    if (args.length > 0) {
      // If arguments provided, use custom message
      String customMessage = String.join(" ", args);
      helloWorld(customMessage);
    } else {
      // Default hello world
      helloWorld();
    }
  }

  /**
   * Prints "Hello, World!" to the console.
   *
   * @return the Hello World message
   */
  public static String helloWorld() {
    String message = "Hello, World!";
    System.out.println(message);
    return message;
  }

  /**
   * Prints a custom hello message to the console.
   *
   * @param name the name or message to greet
   * @return the custom hello message
   */
  public static String helloWorld(String name) {
    if (name == null || name.trim().isEmpty()) {
      return helloWorld();
    }
    String message = "Hello, " + name + "!";
    System.out.println(message);
    return message;
  }

  /**
   * Gets the Hello World message without printing.
   *
   * @return the Hello World message string
   */
  public static String getHelloWorldMessage() {
    return "Hello, World!";
  }
}
