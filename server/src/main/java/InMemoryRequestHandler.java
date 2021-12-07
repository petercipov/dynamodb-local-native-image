import com.amazonaws.services.dynamodbv2.local.server.DynamoDBRequestHandler;
import com.amazonaws.services.dynamodbv2.local.shared.access.awssdkv1.client.LocalAmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.shared.access.awssdkv1.client.LocalAmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.BatchExecuteStatementRequest;
import com.amazonaws.services.dynamodbv2.model.BatchExecuteStatementResult;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeLimitsRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeLimitsResult;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTimeToLiveRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTimeToLiveResult;
import com.amazonaws.services.dynamodbv2.model.ExecuteStatementRequest;
import com.amazonaws.services.dynamodbv2.model.ExecuteStatementResult;
import com.amazonaws.services.dynamodbv2.model.ExecuteTransactionRequest;
import com.amazonaws.services.dynamodbv2.model.ExecuteTransactionResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsResult;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ListTagsOfResourceRequest;
import com.amazonaws.services.dynamodbv2.model.ListTagsOfResourceResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TagResourceRequest;
import com.amazonaws.services.dynamodbv2.model.TagResourceResult;
import com.amazonaws.services.dynamodbv2.model.TransactGetItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactGetItemsResult;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsRequest;
import com.amazonaws.services.dynamodbv2.model.TransactWriteItemsResult;
import com.amazonaws.services.dynamodbv2.model.UntagResourceRequest;
import com.amazonaws.services.dynamodbv2.model.UntagResourceResult;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTableResult;
import com.amazonaws.services.dynamodbv2.model.UpdateTimeToLiveRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTimeToLiveResult;

import java.util.concurrent.ConcurrentHashMap;

public class InMemoryRequestHandler extends DynamoDBRequestHandler {

    private final ConcurrentHashMap<String, CreateTableRequest> schemas = new ConcurrentHashMap<>();
    private final LocalAmazonDynamoDB localAmazonDynamoDB;
    private final LocalAmazonDynamoDBStreams localAmazonDynamoDBStreams;


    public InMemoryRequestHandler(final int authorityLevel, LocalAmazonDynamoDB localAmazonDynamoDB, LocalAmazonDynamoDBStreams localAmazonDynamoDBStreams) {
        super(authorityLevel);
        this.localAmazonDynamoDB = localAmazonDynamoDB;
        this.localAmazonDynamoDBStreams = localAmazonDynamoDBStreams;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public BatchGetItemResult batchGetItem(final String accessKey, final String region,
                                           final BatchGetItemRequest batchGetItemRequest) {
        return localAmazonDynamoDB.batchGetItem(batchGetItemRequest);
    }

    @Override
    public BatchWriteItemResult batchWriteItem(final String accessKey, final String region,
                                               final BatchWriteItemRequest batchWriteItemRequest) {
        return localAmazonDynamoDB.batchWriteItem(batchWriteItemRequest);
    }

    @Override
    public TransactWriteItemsResult transactWriteItems(final String accessKey, final String region,
                                                       final TransactWriteItemsRequest transactWriteItemsRequest) {
        return localAmazonDynamoDB.transactWriteItems(transactWriteItemsRequest);
    }

    @Override
    public TransactGetItemsResult transactGetItems(final String accessKey, final String region,
                                                   final TransactGetItemsRequest transactGetItemsRequest) {
        return localAmazonDynamoDB.transactGetItems(transactGetItemsRequest);
    }

    @Override
    public CreateTableResult createTable(final String accessKey, final String region,
                                         final CreateTableRequest createTableRequest) {
        return localAmazonDynamoDB.createTable(createTableRequest);
    }

    @Override
    public DeleteItemResult deleteItem(final String accessKey, final String region,
                                       final DeleteItemRequest deleteItemRequest) {
        return localAmazonDynamoDB.deleteItem(deleteItemRequest);
    }

    @Override
    public DeleteTableResult deleteTable(final String accessKey, final String region,
                                         final DeleteTableRequest deleteTableRequest) {
        return localAmazonDynamoDB.deleteTable(deleteTableRequest);
    }

    @Override
    public DescribeStreamResult describeStream(final String accessKey, final String region,
                                               final DescribeStreamRequest describeStreamRequest) {
        return localAmazonDynamoDBStreams.describeStream(describeStreamRequest);
    }

    @Override
    public DescribeTableResult describeTable(final String accessKey, final String region,
                                             final DescribeTableRequest describeTableRequest) {
        return localAmazonDynamoDB.describeTable(describeTableRequest);
    }

    @Override
    public DescribeLimitsResult describeLimits(final String accessKey, final String region,
                                               final DescribeLimitsRequest describeLimitsRequest) {
        return localAmazonDynamoDB.describeLimits(describeLimitsRequest);
    }

    @Override
    public DescribeTimeToLiveResult describeTimeToLive(final String accessKey, final String region,
                                                       final DescribeTimeToLiveRequest describeTimeToLiveRequest) {
        return localAmazonDynamoDB.describeTimeToLive(describeTimeToLiveRequest);
    }

    @Override
    public GetItemResult getItem(final String accessKey, final String region,
                                 final GetItemRequest getItemRequest) {
        return localAmazonDynamoDB.getItem(getItemRequest);
    }

    @Override
    public GetShardIteratorResult getShardIterator(final String accessKey, final String region,
                                                   final GetShardIteratorRequest getShardIteratorRequest) {
        return localAmazonDynamoDBStreams.getShardIterator(getShardIteratorRequest);
    }

    @Override
    public GetRecordsResult getRecords(final String accessKey, final String region,
                                       final GetRecordsRequest getRecordsRequest) {
        return localAmazonDynamoDBStreams.getRecords(getRecordsRequest);
    }

    @Override
    public ListStreamsResult listStreams(final String accessKey, final String region,
                                         final ListStreamsRequest listStreamsRequest) {
        return localAmazonDynamoDBStreams.listStreams(listStreamsRequest);
    }

    @Override
    public ListTablesResult listTables(final String accessKey, final String region,
                                       final ListTablesRequest listTablesRequest) {
        return localAmazonDynamoDB.listTables(listTablesRequest);
    }

    @Override
    public ListTagsOfResourceResult listTagsOfResource(final String accessKey, final String region,
                                                       final ListTagsOfResourceRequest listTagsOfResourceRequest) {
        return localAmazonDynamoDB.listTagsOfResource(listTagsOfResourceRequest);
    }

    @Override
    public PutItemResult putItem(final String accessKey, final String region,
                                 final PutItemRequest putItemRequest) {
        return localAmazonDynamoDB.putItem(putItemRequest);
    }

    @Override
    public QueryResult query(final String accessKey, final String region,
                             final QueryRequest queryRequest) {
        return localAmazonDynamoDB.query(queryRequest);
    }

    @Override
    public ScanResult scan(final String accessKey, final String region,
                           final ScanRequest scanRequest) {
        return localAmazonDynamoDB.scan(scanRequest);
    }

    @Override
    public TagResourceResult tagResource(final String accessKey, final String region,
                                         final TagResourceRequest tagResourceRequest) {
        return localAmazonDynamoDB.tagResource(tagResourceRequest);
    }

    @Override
    public UntagResourceResult untagResource(final String accessKey, final String region,
                                             final UntagResourceRequest untagResourceRequest) {
        return localAmazonDynamoDB.untagResource(untagResourceRequest);
    }

    @Override
    public UpdateItemResult updateItem(final String accessKey, final String region,
                                       final UpdateItemRequest updateItemRequest) {
        return localAmazonDynamoDB.updateItem(updateItemRequest);
    }

    @Override
    public UpdateTableResult updateTable(final String accessKey, final String region,
                                         final UpdateTableRequest updateTableRequest) {
        return localAmazonDynamoDB.updateTable(updateTableRequest);
    }

    @Override
    public UpdateTimeToLiveResult updateTimeToLive(final String accessKey, final String region,
                                                   final UpdateTimeToLiveRequest updateTimeToLiveRequest) {
        return localAmazonDynamoDB.updateTimeToLive(updateTimeToLiveRequest);
    }

    @Override
    public ExecuteStatementResult executeStatement(final String accessKey, final String region,
                                                   final ExecuteStatementRequest executeStatementRequest) {
        return localAmazonDynamoDB.executeStatement(executeStatementRequest);
    }

    @Override
    public BatchExecuteStatementResult batchExecuteStatement(final String accessKey, final String region,
                                                             final BatchExecuteStatementRequest batchExecuteStatementRequest) {
        return localAmazonDynamoDB.batchExecuteStatement(batchExecuteStatementRequest);
    }

    @Override
    public ExecuteTransactionResult executeTransaction(final String accessKey, final String region,
                                                       final ExecuteTransactionRequest executeTransactionRequest) {
        return localAmazonDynamoDB.executeTransaction(executeTransactionRequest);
    }
}
