// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza.util;

import junit.framework.TestCase;

/**
 * Test class NetezzaUtil.
 */
public class TestNetezzaUtil extends TestCase {

  public void testNullSafeCompareTo() {
    assertTrue(NetezzaUtil.nullSafeCompareTo(null, null));
    assertFalse(NetezzaUtil.nullSafeCompareTo(null, "a"));
    assertFalse(NetezzaUtil.nullSafeCompareTo("a", null));
    assertTrue(NetezzaUtil.nullSafeCompareTo("a", "a"));
  }

  public void testRemoveEscapeCharacters() {
    assertEquals("\\N", NetezzaUtil.removeEscapeCharacters("\\\\N"));
    assertEquals("\n", NetezzaUtil.removeEscapeCharacters("\\n"));
    assertEquals("\b", NetezzaUtil.removeEscapeCharacters("\\b"));
    assertEquals("\t", NetezzaUtil.removeEscapeCharacters("\\t"));
    assertEquals("\f", NetezzaUtil.removeEscapeCharacters("\\f"));
    assertEquals("\'", NetezzaUtil.removeEscapeCharacters("\\'"));
    assertEquals("\"", NetezzaUtil.removeEscapeCharacters("\\\""));
    assertEquals("jarcec", NetezzaUtil.removeEscapeCharacters("jarcec"));
  }
}
