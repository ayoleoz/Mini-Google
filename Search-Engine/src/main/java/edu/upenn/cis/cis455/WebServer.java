package edu.upenn.cis.cis455;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Level;

import static spark.Spark.*;
import spark.Spark;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.Table;

public class WebServer {

    private static final Logger logger = LogManager.getLogger(WebServer.class);

    public static void main(String[] args) {
        org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn.cis.cis455", Level.DEBUG);

        int port = Integer.parseInt(args[0]);
        String indexTableName = args[1];
        String pagerankTableName = args[2];
        String documentTableName = args[3];

        Spark.port(port);

        ProfileCredentialsProvider profileCredentialsProvider = new ProfileCredentialsProvider();
        try {
            profileCredentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException("Cannot validating profile credentials: ", e);
        }

        AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_1).withCredentials(profileCredentialsProvider).build();

        DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);

        logger.info("Amazon AWS Credentials configured successfully");

        Table indexTable = dynamoDB.getTable(indexTableName);
        Table pagerankTable = dynamoDB.getTable(pagerankTableName);
        Table documentTable = dynamoDB.getTable(documentTableName);

        logger.debug("Sever starting on port: " + port);
        
        options("/*", (request, response) -> {
            String headers = request.headers("Access-Control-Request-Headers");
            if (headers != null) {
                response.header("Access-Control-Allow-Headers", headers);
            }

            String method = request.headers("Access-Control-Request-Method");
            if (method != null) {
                response.header("Access-Control-Allow-Methods", method);
            }
            return "OK";
        });

        before((request, response) -> {
            response.header("Access-Control-Allow-Origin", "*");
            response.header("Access-Control-Allow-Methods", "GET");
        });

        get("/hello", (request, response) -> "hello world!");

        get("/search", new Query(documentTable, indexTable, pagerankTable, dynamoDB));
    }
}
