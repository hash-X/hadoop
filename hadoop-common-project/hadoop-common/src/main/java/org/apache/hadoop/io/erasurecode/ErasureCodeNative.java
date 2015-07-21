/*
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.NativeCodeLoader;

/**
 * Erasure code native libraries (for now, Intel ISA-L) related utilities.
 */
public class ErasureCodeNative {

  private static final Log LOG =
      LogFactory.getLog(ErasureCodeNative.class.getName());

  private static boolean nativeIsalLoaded = false;

  static {
    if (NativeCodeLoader.isNativeCodeLoaded() &&
        NativeCodeLoader.buildSupportsIsal()) {
      try {
        loadLibrary();
        nativeIsalLoaded = true;
      } catch (Throwable t) {
        LOG.error("failed to load ISA-L", t);
      }
    }
  }

  /**
   * Are native libraries loaded?
   */
  public static boolean isNativeCodeLoaded() {
    return nativeIsalLoaded;
  }

  /**
   * Is the native ISA-L library loaded & initialized? Throw exception if not.
   */
  public static void checkNativeCodeLoaded() {
    if (!nativeIsalLoaded) {
      throw new RuntimeException("Native ISA-L library not available: " +
          "this version of libhadoop was built without " +
          "ISA-L support.");
    }
  }

  /**
   * Load native library available or supported
   */
  public static native void loadLibrary();

  /**
   * Get the native library name that's available or supported.
   */
  public static native String getLibraryName();

  /**
   * Verify the native library work or not probably by a quick test, to be used
   * by Java unit test. Call this after make sure native library is supported
   * and also loaded. It will report meaningful error if any thru exception.
   * Please DONT call this in production codes because it may run some time to
   * detect concrete error.
   */
  public static native void verifyTest();
}
