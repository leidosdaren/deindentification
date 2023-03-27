package example;


import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;

import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.S3Client;

import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3EventNotificationRecord;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.regions.Region;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Handler value: example.Handler
public class Handler implements RequestHandler<S3Event, String> {
  private static final Logger logger = LoggerFactory.getLogger(Handler.class);
  static Map<String, String> tokenMap = new HashMap<>();
  public static final int SSN_FIELD = 1; 
  
  private String DYNAMODB_TABLE_NAME = "ssnMap";
  private Region REGION = Region.US_EAST_1;

  @Override
  public String handleRequest(S3Event s3event, Context context) {
    String currentLine;
    String[] splitLine;
    String subToken;
    String piiToken;
    Random rnd = new Random();
    
    try {

      /*
       * Setup the DynamoDB client and enhanced client
       */
      DynamoDbClient ddbClient = DynamoDbClient.builder()
                .region(REGION)
                .build();
      
      /*
       * Setup the S3Event notification, src/dest buckets, and the S3 client
       */
      S3EventNotificationRecord record = s3event.getRecords().get(0);
      
      String srcBucket = record.getS3().getBucket().getName();

      // Object key may have spaces or unicode non-ASCII characters.
      String srcKey = record.getS3().getObject().getUrlDecodedKey();

      String dstBucket = srcBucket;
      String dstKey = "tokenized-" + srcKey;

      // Download the image from S3 into a stream
      S3Client s3Client = S3Client.builder().build();
      InputStream s3Object = getObject(s3Client, srcBucket, srcKey);

      InputStreamReader inputStreamReader = new InputStreamReader(s3Object);
      BufferedReader reader = new BufferedReader(inputStreamReader);
      ByteArrayOutputStream tokenizedOut = new ByteArrayOutputStream();

      while ((currentLine = reader.readLine()) != null)
      {
        System.out.println("orig: " + currentLine);
        splitLine = currentLine.split(",");
        piiToken = splitLine[SSN_FIELD];
        //subToken = getTokenSubstitute(piiToken);
        System.out.println("Getting token for pii: " + piiToken);
        subToken = getFromTokenMap(ddbClient, DYNAMODB_TABLE_NAME, "ssn", piiToken);

        // getToken returns null if we haven't seen the PII token before
        // if we've seen the PII token, it returns the substitute token
        if (subToken != null) {
            // retToken isn't null so getToken returned the sub value
            splitLine[SSN_FIELD] = subToken;
            String newLine = String.join(",",splitLine);
            newLine = newLine + "\n";
            tokenizedOut.write(newLine.getBytes());
        }
        else {
            // have not seen this retToken (it's null) so gen a new token, save it, 
            // sub it in for the tokenized file
            int newToken = rnd.nextInt(999999999);
            String newTokenStr = String.format("%09d", newToken);
            //storeNewToken(piiToken, newTokenStr);
            persistToTokenMap(ddbClient, DYNAMODB_TABLE_NAME, "ssn", piiToken, "token", newTokenStr);
            splitLine[SSN_FIELD] = newTokenStr;
            String newLine = String.join(",",splitLine);
            newLine = newLine + "\n";
            tokenizedOut.write(newLine.getBytes());
        }
      }
      inputStreamReader.close();
      
      // Upload tokenized file to S3
      putObject(s3Client, tokenizedOut, dstBucket, dstKey);

      logger.info("Successfully resized " + srcBucket + "/"
              + srcKey + " and uploaded to " + dstBucket + "/" + dstKey);
      return "Ok";
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /*
   * persistToTokenMap
   *  tableName - The DynamoDB table that holds the token mapping
   *  key - the PII key for the token map (i.e. 'ssn' or 'caseId')
   *  keyVal - the actual value of the PII key to be stored in the table 
   *  token - the name for the value that will be sub'ed in (i.e. token)
   *  tokenVal - the value of the token to be sub'ed in (the generated value that is used to remove the PII)
   */
  public static void persistToTokenMap(DynamoDbClient ddb,
                                      String tableName,
                                      String key,
                                      String keyVal,
                                      String token,
                                      String tokenVal){

        HashMap<String,AttributeValue> itemValues = new HashMap<>();
        itemValues.put(key, AttributeValue.builder().s(keyVal).build());
        itemValues.put(token, AttributeValue.builder().s(tokenVal).build());

        PutItemRequest request = PutItemRequest.builder()
            .tableName(tableName)
            .item(itemValues)
            .build();

        try {
            PutItemResponse response = ddb.putItem(request);
            System.out.println(tableName +" was successfully updated. The request id is "+response.responseMetadata().requestId());

        } catch (ResourceNotFoundException e) {
            System.err.format("Error: The Amazon DynamoDB table \"%s\" can't be found.\n", tableName);
            System.err.println("Be sure that it exists and that you've typed its name correctly!");
            System.exit(1);
        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    /*
    * getFromTokenMap
    *  ddb - DynamoDBClient
    *  tableName - The DynamoDB table that holds the token mapping
    *  key - the PII key for the token map (i.e. 'ssn' or 'caseId')
    *  keyVal - the actual value of the PII key to be stored in the table 
    */
    public static String getFromTokenMap(DynamoDbClient ddb,String tableName,String key,String keyVal ) {

      HashMap<String,AttributeValue> keyToGet = new HashMap<>();
      keyToGet.put(key, AttributeValue.builder()
          .s(keyVal)
          .build());

      GetItemRequest request = GetItemRequest.builder()
          .key(keyToGet)
          .tableName(tableName)
          .build();

      try {
          Map<String,AttributeValue> returnedItem = ddb.getItem(request).item();
          if (returnedItem.containsKey("token")) {
            System.out.println("In getFromTokenMap, key: " + key + ", keyVal: " + keyVal);
            System.out.println("Token returned: " + returnedItem.get("token").toString());
            return returnedItem.get("token").s();
            
          } else {
              return null;
          }

      } catch (DynamoDbException e) {
          System.err.println(e.getMessage());
          System.exit(1);
      }
      return null;
  }

  private InputStream getObject(S3Client s3Client, String bucket, String key) {
    GetObjectRequest getObjectRequest = GetObjectRequest.builder()
      .bucket(bucket)
      .key(key)
      .build();
    return s3Client.getObject(getObjectRequest);
  }

  /*
  private static String getTokenSubstitute(String piiToken) {
    return(tokenMap.get(piiToken));

  }

  private static void storeNewToken(String piiToken, String newToken) {
    tokenMap.put(piiToken, newToken);
  }
  */

  private void putObject(S3Client s3Client, ByteArrayOutputStream outputStream,
    String bucket, String key) {
      

      PutObjectRequest putObjectRequest = PutObjectRequest.builder()
        .bucket(bucket)
        .key(key)
        .build();

      // Uploading to S3 destination bucket
      logger.info("Writing to: " + bucket + "/" + key);
      try {
        s3Client.putObject(putObjectRequest,
          RequestBody.fromBytes(outputStream.toByteArray()));
      }
      catch(AwsServiceException e)
      {
        logger.error(e.awsErrorDetails().errorMessage());
        System.exit(1);
      }
  }

  
}
