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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io.erasurecode;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
<<<<<<< HEAD:hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/io/erasurecode/TestNativeErasureCode.java
 * Test native erasure code library.
=======
 * Container property encoding allocation and execution semantics.
 * 
 * <p>
 * The container types are the following:
 * <ul>
 * <li>{@link #APPLICATION_MASTER}
 * <li>{@link #TASK}
 * </ul>
>>>>>>> 76957a485b526468498f93e443544131a88b5684:hadoop-yarn-project/hadoop-yarn/hadoop-yarn-api/src/main/java/org/apache/hadoop/yarn/server/api/ContainerType.java
 */
public class TestNativeErasureCode {

  @Before
  public void before() {
    Assume.assumeTrue(ErasureCodeNative.isNativeCodeLoaded());
  }

  @Test
  public void testNativeLibrry() {
    ErasureCodeNative.verifyTest();
  }
}
