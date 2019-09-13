package io.cdap.plugin.facebook.source;

import com.facebook.ads.sdk.APIException;
import com.facebook.ads.sdk.AdsActionStats;
import com.facebook.ads.sdk.AdsInsights;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Docs.
 */
public class TestAPI {

  static class Property {
    String id;
    String label;

    Property(String id, String label) {
      this.id = id;
      this.label = label;
    }
  }

  public static void main(String[] args) throws APIException {
    Gson gson = new Gson();

    Class c = AdsInsights.class;

    List<String> stringFields = Arrays.stream(c.getDeclaredFields())
      .filter((field -> field.isAnnotationPresent(SerializedName.class) && field.getType().equals(String.class)))
      .map(field -> field.getAnnotation(SerializedName.class).value())
      .collect(Collectors.toList());

    stringFields.forEach(s -> {
      System.out.println(String.format("case \"%s\":", s));
    });
    System.out.println("return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.STRING)));");

    List<String> actionStatsFields = Arrays.stream(c.getDeclaredFields())
      .filter((field -> field.isAnnotationPresent(SerializedName.class)
        && field.getType().equals(AdsActionStats.class)))
      .map(field -> field.getAnnotation(SerializedName.class).value()
      ).collect(Collectors.toList());

    actionStatsFields.forEach(s -> {
      System.out.println(String.format("case \"%s\":", s));
    });
    System.out.println("return Schema.Field.of(name, Schema.nullableOf(ADS_ACTION_STATS_SCHEMA));");

    List<String> actionListFields = Arrays.stream(c.getDeclaredFields())
      .filter((field -> field.isAnnotationPresent(SerializedName.class) && field.getType().equals(List.class)))
      .map(field -> {
        if (field.getGenericType().getTypeName().contains("AdsActionStats")) {
          return field.getAnnotation(SerializedName.class).value();
        } else {
          return null;
        }
      })
      .filter(Objects::nonNull)
      .collect(Collectors.toList());

    actionListFields.forEach(s -> {
      System.out.println(String.format("case \"%s\":", s));
    });
    System.out.println("return Schema.Field.of(name, Schema.nullableOf(Schema.arrayOf(ADS_ACTION_STATS_SCHEMA)));");

//    String access_token = "EAAFLnZA9K2QYBALcdja79WFvrCl3gdIcdc42NGrD6UoR9ZA8AR116wXFdYyehYZAjDOVTWxo1KTtYCxtttgeWC" +
//      "DYS4GxzdZAhOZC8RMYjynsA9pJ7JxKgK7NgbyOjn1H57QRv1DeB24PQmevecc0eVlYiDQZA4ZAEWB5p5a9IvbQKiOZBCjq90vAr0ZCUmmIU" +
//      "CUkZD";
//    String app_secret = "<APP_SECRET>";
//    String app_id = "<APP_ID>";
//    String id = "120330000173688107";
//    APIContext context = new APIContext(access_token).enableDebug(true);
//    APINodeList<AdsInsights> result  = new AdSet(id, context).getInsights()
//      .requestField("impressions")
//      .requestField("spend")
//      .execute(ImmutableMap.of("limit", "1"));
//    System.out.println(result);
  }
}
