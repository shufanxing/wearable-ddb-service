package com.shufan.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.core.Response;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class Database {

    static int DEFAULT_MAX_DAY = -1;

    static String AWS_ACCESS_KEY = "";
    static String AWS_SECRET_KEY = "";

    AWSCredentials awsCreds = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY);

	ClientConfiguration config = new ClientConfiguration().withMaxConnections(10).withConnectionTimeout(1000);
//	AmazonDynamoDB ddbClient = AmazonDynamoDBClientBuilder.standard()
//			.withCredentials(new AWSStaticCredentialsProvider(awsCreds))
//			.withClientConfiguration(config)
//			.withRegion(Regions.US_WEST_2)
//			.build();
    
	AmazonDynamoDB ddbClient = AmazonDynamoDBClientBuilder.standard()
            .withRegion(Regions.US_WEST_2)
			.withCredentials(new AWSStaticCredentialsProvider(awsCreds))
			.withClientConfiguration(config)
			.build();
//    AmazonDynamoDBAsync  ddbClient = AmazonDynamoDBAsyncClientBuilder.standard()
//            .withRegion(Regions.US_WEST_2)
//            .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
//            //	        .withExecutorFactory(() -> Executors.newFixedThreadPool(10))
//            .build();

    String tableName = "StepCount";

    public Response insertItem(int user_id, int day, int time_interval, int step_count) {

        try {

            String dtStr = String.format("%02d%02d", day, time_interval);

            Map<String, AttributeValue> item = new HashMap<>();
            item.put("UserID", new AttributeValue().withS(String.valueOf(user_id)));
            item.put("DateHour", new AttributeValue().withS(dtStr));
            item.put("Step", new AttributeValue().withS(String.valueOf(step_count)));

//        Future<PutItemResult> future = ddbClient.putItemAsync(tableName, item);
//                PutItemResult result = future.get(500, TimeUnit.MILLISECONDS);
//        return Response.status(200).entity("post success" + result.getSdkHttpMetadata()).build();

            PutItemResult result = ddbClient.putItem(tableName, item);
            return Response.status(200).entity("post success" + result.getSdkHttpMetadata()).build();

        } catch (Exception e) {
            System.err.println("Unable to postStepCount: " + e.getMessage());
            return Response.status(404).entity("Unable to postStepCount: " + e.getMessage()).build();
        }
    }

    public Response queryItem(int user_id, int start_day, int start_time, int end_day, int end_time) {

        try {
            String dt1Str = String.format("%02d%02d", start_day, start_time);
            String dt2Str = String.format("%02d%02d", end_day, end_time);

            Map<String, AttributeValue> map = new HashMap<>();
            map.put(":userid", new AttributeValue().withS(String.valueOf(user_id)));
            map.put(":dt1", new AttributeValue().withS(dt1Str));
            map.put(":dt2", new AttributeValue().withS(dt2Str));

            QueryRequest request = new QueryRequest(tableName)
                    .withKeyConditionExpression("UserID = :userid and DateHour between :dt1 and :dt2")
                    .withExpressionAttributeValues(map);

//        Future<QueryResult> future = ddbClient.queryAsync(request);
//        QueryResult result = future.get(500, TimeUnit.MILLISECONDS);
            QueryResult result = ddbClient.query(request);

            List<Map<String, AttributeValue>> items = result.getItems();
            Iterator<Map<String, AttributeValue>> iterator = items.iterator();
            int total = 0;

            while (iterator.hasNext()) {
                Map<String, AttributeValue> item = iterator.next();
                total += Integer.valueOf(item.get("Step").getN());
            }

            return Response.status(200).entity("getRange" + total).build();

        } catch (Exception e) {
            System.err.println("Unable to getCurrent: " + e.getMessage());
            return Response.status(404).entity("Unable to getCurrent: " + e.getMessage()).build();
        }

    }

    public int queryCurrentDay(int user_id) throws InterruptedException, ExecutionException, TimeoutException {

        Map<String, AttributeValue> map = new HashMap<>();
        map.put(":userid", new AttributeValue().withS(String.valueOf(user_id)));

        QueryRequest request = new QueryRequest(tableName)
                .withKeyConditionExpression("UserID = :userid")
                .withExpressionAttributeValues(map);

//        Future<QueryResult> future = ddbClient.queryAsync(request);
//        QueryResult result = future.get(2000, TimeUnit.MILLISECONDS);
        QueryResult result = ddbClient.query(request);

        List<Map<String, AttributeValue>> items = result.getItems();
        Iterator<Map<String, AttributeValue>> iterator = items.iterator();
        int maxDay = DEFAULT_MAX_DAY;

        while (iterator.hasNext()) {
            Map<String, AttributeValue> item = iterator.next();
            String date = item.get("DateHour").getS().substring(0, 2);
            maxDay = Math.max(maxDay, Integer.valueOf(date));
        }

        return maxDay;

    }

    public Response deleteAll(int readCapacity, int writeCapacity) {
        try {
            dropTable();
            return createTable(readCapacity, writeCapacity);
        } catch (Exception e) {
            return Response.status(404).entity(e.getMessage()).build();
        }
    }

    public Response createTable(int readCapacity, int writeCapacity) {
        try {
            List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
            attributeDefinitions.add(new AttributeDefinition("UserID", "S"));
            attributeDefinitions.add(new AttributeDefinition("DateHour", "S"));

            List<KeySchemaElement> keySchema = new ArrayList<>();
            keySchema.add(new KeySchemaElement("UserID", "HASH"));
            keySchema.add(new KeySchemaElement("DateHour", "RANGE"));

//        	Future<CreateTableResult> future = ddbClient.createTableAsync(attributeDefinitions, tableName, keySchema, new ProvisionedThroughput((long)readCapacity, (long)writeCapacity));
//        	CreateTableResult result = future.get();
            CreateTableResult result = ddbClient.createTable(attributeDefinitions, tableName, keySchema, new ProvisionedThroughput((long) readCapacity, (long) writeCapacity));

            return Response.status(200).entity(result.getSdkHttpMetadata()).build();
        } catch (Exception e) {
            return Response.status(404).entity(e.getMessage()).build();
        }

    }

    public Response dropTable() {


        try {
        	
            DeleteTableResult result = ddbClient.deleteTable(tableName);

//    		Future<DeleteTableResult> future = ddbClient.deleteTableAsync(tableName);
//    		DeleteTableResult result = future.get();
            
            return Response.status(200).entity(result.getSdkHttpMetadata()).build();
        } catch (Exception e) {
            return Response.status(404).entity(e.getMessage()).build();
        }

    }
}
