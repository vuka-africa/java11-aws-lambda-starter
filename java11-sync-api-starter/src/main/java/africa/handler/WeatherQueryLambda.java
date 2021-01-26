package africa.handler;

import africa.model.WeatherItem;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedScanList;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class WeatherQueryLambda
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

    try {
      final String json = objectMapper.writeValueAsString(this.findAll());

      return response.withStatusCode(200).withBody(json);
    } catch (IOException e) {
      return response.withBody("{}").withStatusCode(500);
    }
  }

  public List<WeatherItem> findAll() {
    PaginatedScanList<WeatherItem> scan =
        mapper.scan(WeatherItem.class, new DynamoDBScanExpression());
    List<WeatherItem> weatherItemList = scan.stream().collect(Collectors.toList());
    return weatherItemList;
  }
}
