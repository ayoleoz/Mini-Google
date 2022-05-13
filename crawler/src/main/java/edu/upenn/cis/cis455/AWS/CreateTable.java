package edu.upenn.cis.cis455.AWS;

import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * To run this Java V2 code example, ensure that you have setup your development
 * environment, including your credentials.
 *
 * For information, see this documentation topic:
 *
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */
public class CreateTable {

    public static void main(String[] args) {
        // String result = createSimpleTable(DynamoDB.CONTENT_SEEN, "hash");
        // String result = createSimpleTable(DynamoDB.VISITED_URL, "url");
        // createWebLink();
    }

    /**
     * To create a Dynamo DB Table: 1. Provide atble name and the key name 2.
     * Provide Attribute Type of the key (S for String) 3. Select key type (most
     * likely it would be HASH = partition Key)
     * 
     * @param tableName
     * @param keyName
     * @return
     */
    public static String createSimpleTable(String tableName, String keyName) {
        DynamoDbClient ddb = DynamoDB.getDynamoDBClient();
        CreateTableRequest request = CreateTableRequest.builder()
                .attributeDefinitions(AttributeDefinition.builder().attributeName(keyName)
                        .attributeType(ScalarAttributeType.S).build())
                .keySchema(KeySchemaElement.builder().attributeName(keyName).keyType(KeyType.HASH).build())
                .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(new Long(10))
                        .writeCapacityUnits(new Long(10)).build())
                .tableName(tableName).build();
        String tableId = "";
        try {
            CreateTableResponse result = ddb.createTable(request);
            tableId = result.tableDescription().tableId();
            return tableId;
        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return "";
    }

    /**
     * To create a table with composite key: a partition key, with KeyType HASH; and
     * a sort key, with key type RANGE
     * 
     * @param tableName
     * @param partitionKey
     * @param sortKey
     * @return
     */
    public static void createWebLink() {
        DynamoDB.getTable(DynamoDB.WEB_LINKS, DynamoDB.WEB_LINKS_SCHEMA).createTable();
    }

}
