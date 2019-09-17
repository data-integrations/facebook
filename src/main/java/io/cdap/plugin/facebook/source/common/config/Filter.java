package io.cdap.plugin.facebook.source.common.config;

public class Filter {
  String field;
  String operator;
  String value;

  public Filter(String field, String operator, String value) {
    this.field = field;
    this.operator = operator;
    this.value = value;
  }

  public String getField() {
    return field;
  }

  public String getOperator() {
    return operator;
  }

  public String getValue() {
    return value;
  }
}
