package africa.makola;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class WeatherEventLambda
    implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
  private final ObjectMapper objectMapper = new ObjectMapper();

  final AmazonDynamoDB client =
      AmazonDynamoDBClientBuilder.standard().withRegion(Regions.AP_SOUTH_1).build();
  final DynamoDBMapper mapper = new DynamoDBMapper(client);

  @Override
  public APIGatewayProxyResponseEvent handleRequest(
      APIGatewayProxyRequestEvent apiGatewayProxyRequestEvent, Context context) {
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", "application/json");
    headers.put("X-Custom-Header", "application/json");

    APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent().withHeaders(headers);

    System.out.println("APIGatewayProxyRequestEvent:  \n" + apiGatewayProxyRequestEvent.toString());
    WeatherEvent weatherEvent;

    try {
      weatherEvent =
          objectMapper.readValue(apiGatewayProxyRequestEvent.getBody(), WeatherEvent.class);
      System.out.println("WeatherEvent:  \n" + weatherEvent.toString());

      WeatherItem item =
          new WeatherItem(
              weatherEvent.locationName,
              weatherEvent.temperature,
              weatherEvent.timestamp,
              weatherEvent.longitude,
              weatherEvent.latitude);

      System.out.println("WeatherItem:  \n" + item.toString());
      mapper.save(item);
      return response.withStatusCode(200).withBody(weatherEvent.locationName);
    } catch (IOException e) {
      return response.withBody("{}").withStatusCode(500);
    }
  }
}
