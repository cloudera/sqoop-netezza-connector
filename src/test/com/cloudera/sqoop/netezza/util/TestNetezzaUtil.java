// (c) Copyright 2010 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.netezza.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test class NetezzaUtil.
 */
public class TestNetezzaUtil {

  @Test
  public void testNullSafeCompareTo() {
    assertTrue(NetezzaUtil.nullSafeCompareTo(null, null));
    assertFalse(NetezzaUtil.nullSafeCompareTo(null, "a"));
    assertFalse(NetezzaUtil.nullSafeCompareTo("a", null));
    assertTrue(NetezzaUtil.nullSafeCompareTo("a", "a"));
  }

  @Test
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
