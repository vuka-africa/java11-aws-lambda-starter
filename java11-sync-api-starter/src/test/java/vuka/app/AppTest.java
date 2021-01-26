package vuka.app;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import africa.handler.HelloMakolaLambda;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import org.junit.Test;

public class AppTest {
  @Test
  public void successfulResponse() {
    HelloMakolaLambda app = new HelloMakolaLambda();
    APIGatewayProxyResponseEvent result = app.handleRequest(null, null);
    assertEquals(result.getStatusCode().intValue(), 200);
    assertEquals(result.getHeaders().get("Content-Type"), "application/json");
    String content = result.getBody();
    assertNotNull(content);
    assertTrue(content.contains("\"message\""));
    assertTrue(content.contains("\"hello world\""));
    assertTrue(content.contains("\"location\""));
  }
}
