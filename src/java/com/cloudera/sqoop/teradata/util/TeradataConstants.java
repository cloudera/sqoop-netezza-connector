// (c) Copyright 2011 Cloudera, Inc. All Rights Reserved.

package com.cloudera.sqoop.teradata.util;

/**
 * A Utility class maintaining constants used for the Teradata connector.
 */
public class TeradataConstants {

  protected TeradataConstants(){
    // prevents calls from subclass
    throw new UnsupportedOperationException();
  }
  
  public static final String MULTI_INSERT_STATEMENTS = "multi.insert."
      + "statements";
  public static final String EXPORT_DELETE_TEMP_TABLES = "teradata.export."
      + "delete_temporary_tables";
  public static final String EXPORT_TEMP_TABLES_SUFFIX = "teradata.export."
      + "tables_suffix";
  public static final String IMPORT_DELETE_TEMP_TABLE = "teradata.import."
      + "delete_temporary_table";
  public static final String IMPORT_TEMP_TABLE_SUFFIX = "teradata.import."
      + "table_suffix";
  public static final String INPUT_TABLE_NAME = "input.table.name";
  public static final String TEMP_TABLE_NAME = "temporary.table.name";
  public static final String PARAMETRIZED_QUERY = "parameterized.sql.query";
  public static final String INDEX_COLUMN = "index_column";

}
