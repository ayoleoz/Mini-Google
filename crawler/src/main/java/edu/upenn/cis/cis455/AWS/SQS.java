package edu.upenn.cis.cis455.AWS;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import edu.upenn.cis.cis455.crawler.utils.HashFunc;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SqsException;

public class SQS {
    final static Logger logger = LogManager.getLogger(SQS.class);
    // configurations
    public static final String QUEUE_ADDR = "https://sqs.us-east-1.amazonaws.com/751082190059/wiki.fifo";
    public static final String QUEUE_TWO_ADDR = "https://sqs.us-east-1.amazonaws.com/751082190059/non_wiki.fifo";
    public static final SqsClient client = getSqsClient();

    /**
     * Get a SQS Client
     */
    public static SqsClient getSqsClient() {
        return SqsClient.builder().credentialsProvider(StaticCredentialsProvider.create(Utils.awsCreds))
                .region(Region.US_EAST_1).build();
    }

    /**
     * Receive messages posted in SQS queue
     */
    public static List<Message> receiveMessages(String queueAddr) {
        logger.info("\nReceive messages from queue " + queueAddr);
        try {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder().queueUrl(queueAddr)
                    .maxNumberOfMessages(10).build();
            List<Message> messages = client.receiveMessage(receiveMessageRequest).messages();
            return messages;
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return null;
    }

    /**
     * Delete messages that have been received and processed
     */
    public static void deleteMessages(List<Message> messages, String queueAddr) {
        logger.info("\nDelete Messages");
        if (messages == null || messages.isEmpty()) {
            return;
        }
        try {
            for (Message message : messages) {
                DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().queueUrl(queueAddr)
                        .receiptHandle(message.receiptHandle()).build();
                DeleteMessageResponse response = client.deleteMessage(deleteMessageRequest);
                logger.info(response.toString());
            }

        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    /**
     * send messages in batch
     */
    public synchronized static void sendBatchMessages(Set<String> msgSet, String queueAddr) {
        List<Set<String>> messages = new ArrayList<>();
        logger.info("\nSend " + msgSet.size() + " messages in batch to queue " + queueAddr);
        if (msgSet.size() > 10) {
            logger.info("Too many messages, need to split into list of sets for batch sending");
            messages = Utils.split(msgSet, 10);
        } else {
            messages.add(msgSet);
        }
        for (Set<String> message : messages) {
            List<SendMessageBatchRequestEntry> entries = new ArrayList<>();
            for (String msg : message) {
                if (msg == null || msg.isEmpty() || msg.isBlank())
                    continue;
                logger.info("add a msg request entry for " + msg);
                String id = HashFunc.MD5(msg);
                SendMessageBatchRequestEntry current = SendMessageBatchRequestEntry.builder().id(id).messageGroupId(id)
                        .messageBody(msg).build();
                entries.add(current);
            }
            try {
                SendMessageBatchRequest sendMessageBatchRequest = SendMessageBatchRequest.builder().queueUrl(queueAddr)
                        .entries(entries).build();
                client.sendMessageBatch(sendMessageBatchRequest);
                logger.info("should've sent urls now");
            } catch (SqsException e) {
                System.err.println(e.awsErrorDetails().errorMessage());
                System.exit(1);
            }
        }
    }
}
