package org.notmysock.benchmark.tpcds;

import java.util.EnumSet;

public enum TpcdsTableProperties {
  TABLE_NAME("tpcds.table", null),
  SCALE("tpcds.scale", "1"),
  PARALLEL("tpcds.parallel", "1");
  

  private final String key;
  private final String defaultValue;
  
  TpcdsTableProperties(String key, String defaultValue) {
    this.key = key;
    this.defaultValue = defaultValue;
  }
  
  public String getKey() {
    return key;
  }

  public String getDefaultValue() {
    return defaultValue;
  }
  
  public static EnumSet<TpcdsTableProperties> getOptions() {
    return EnumSet.allOf(TpcdsTableProperties.class)
        .complementOf(EnumSet.of(TABLE_NAME));
  }
}
