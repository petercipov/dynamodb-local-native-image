import com.amazonaws.services.dynamodbv2.local.shared.access.ListTablesResultInfo;
import com.amazonaws.services.dynamodbv2.local.shared.access.LocalDBAccess;
import com.amazonaws.services.dynamodbv2.local.shared.access.QueryResultInfo;
import com.amazonaws.services.dynamodbv2.local.shared.access.ShardIterator;
import com.amazonaws.services.dynamodbv2.local.shared.access.TableInfo;
import com.amazonaws.services.dynamodbv2.local.shared.access.sqlite.TableSchemaInfo;
import com.amazonaws.services.dynamodbv2.local.shared.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.local.shared.model.Condition;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.IndexStatus;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.StreamDescription;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class InMemoryStore implements LocalDBAccess {

    private final ConcurrentSkipListMap<String, Table> tables = new ConcurrentSkipListMap<>();
    private ReentrantReadWriteLock writeLock = new ReentrantReadWriteLock();

    @Override
    public void createTable(
        final String tableName,
        final AttributeDefinition hashKey,
        final AttributeDefinition rangeKey,
        final List<AttributeDefinition> allAttributes,
        final List<LocalSecondaryIndex> lsiIndexes,
        final List<GlobalSecondaryIndex> gsiIndexes,
        final ProvisionedThroughput throughput,
        final BillingMode billingMode,
        final StreamSpecification streamSpecification) {

        tables.put(tableName, new Table(tableName, hashKey, rangeKey, allAttributes, lsiIndexes, gsiIndexes,
            throughput, billingMode, streamSpecification));
    }

    @Override
    public ListTablesResultInfo listTables(String exclusiveStartTableName,
                                           Long limit) {

        final AtomicBoolean startPresent = new AtomicBoolean(exclusiveStartTableName == null ? true : false);
        final List<String> tableNames = tables.navigableKeySet().stream().
            filter(name -> {
                if (!startPresent.get()) {
                    if (name.equals(exclusiveStartTableName)) {
                        startPresent.set(true);
                        return false;
                    } else {
                        return false;
                    }
                } else {
                    return true;
                }
            })
            .limit(limit == null ? 100 : limit < 0 ? 100 : limit)
            .collect(Collectors.toList());

        final String lastValue = tableNames.isEmpty() ? null : tableNames.get(tableNames.size() - 1);

        return new ListTablesResultInfo(tableNames, lastValue)
            ;
    }

    @Override
    public void deleteTable(final String tableName) {
        tables.remove(tableName);
    }

    @Override
    public void updateTable(final String tableName,
                            final ProvisionedThroughput provisionedThroughput,
                            final BillingMode billingMode,
                            final long lastUpdateToPayPerRequestDateTime,
                            final List<AttributeDefinition> updatedAttributeDefinitions,
                            final List<GlobalSecondaryIndexDescription> gsiIndexes,
                            final StreamSpecification streamSpecification) {

        Optional
            .ofNullable(tables.get(tableName))
            .ifPresent(table -> table.update(updatedAttributeDefinitions, gsiIndexes));
    }

    @Override
    public void updateTable(final String tableName,
                            final String timeToLiveAttributeName) {
        Optional
            .ofNullable(tables.get(tableName))
            .ifPresent(table -> table.update(timeToLiveAttributeName));

    }

    @Override
    public Map<String, AttributeValue> getRecord(final String tableName,
                                                 final Map<String, AttributeValue> primaryKey) {
        return Optional
            .ofNullable(tables.get(tableName))
            .map(table -> table.getRecord(primaryKey))
            .orElseThrow();
    }

    @Override
    public boolean deleteRecord(final String tableName,
                                final Map<String, AttributeValue> primaryKey,
                                final boolean isSystemDelete) {
        return Optional
            .ofNullable(tables.get(tableName))
            .map(table -> table.deleteRecord(primaryKey, isSystemDelete)).orElse(false);
    }

    @Override
    public void putRecord(final String tableName,
                          final Map<String, AttributeValue> record,
                          final AttributeValue hashKey,
                          final AttributeValue rangeKey,
                          final boolean isUpdate) {

        Optional
            .ofNullable(tables.get(tableName))
            .ifPresent(table -> table.putRecord(hashKey, rangeKey, record));
    }

    @Override
    public QueryResultInfo queryRecords(final String tableName,
                                        final String indexName,
                                        final Map<String, Condition> conditions,
                                        final Map<String, AttributeValue> exclusiveStartKey,
                                        final Long limit,
                                        final boolean ascending,
                                        final byte[] beginHash,
                                        final byte[] endHash,
                                        final boolean isScan,
                                        final boolean isGSIIndex) {
        return Optional
            .ofNullable(tables.get(tableName))
            .map(table -> table.queryRecords(indexName, conditions, exclusiveStartKey, limit, ascending, beginHash,
                endHash, isScan, isGSIIndex))
            .orElseThrow();
    }

    @Override
    public long getTableItemCount(final String tableName) {
        return Optional
            .ofNullable(tables.get(tableName))
            .map(table -> table.itemCount())
            .orElseThrow();
    }

    @Override
    public long getLSIItemCount(final String tableName,
                                final String lsiIndexName) {
        return Optional
            .ofNullable(tables.get(tableName))
            .map(table -> table.itemLsiCount(lsiIndexName))
            .orElseThrow();
    }

    @Override
    public long getGSIItemCount(final String tableName,
                                final String gsiIndexName) {
        return Optional
            .ofNullable(tables.get(tableName))
            .map(table -> table.itemGsiCount(gsiIndexName))
            .orElseThrow();
    }

    @Override
    public TableInfo getTableInfo(final String tableName) {
        return Optional
            .ofNullable(tables.get(tableName))
            .map(table -> table.getInfo())
            .orElse(null);
    }

    @Override
    public List<StreamDescription> getStreamInfo(final String tableName,
                                                 final String streamId,
                                                 Integer limit,
                                                 final String exclusiveStartStreamId,
                                                 final String exclusiveStartShardId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        //empty
    }


    @Override
    public long getTableByteSize(final String tableName) {
        return Optional
            .ofNullable(tables.get(tableName))
            .map(table -> table.byteSize())
            .orElseThrow();
    }

    @Override
    public long getLSIByteSize(final String tableName,
                               final String lsiIndexName) {
        return Optional
            .ofNullable(tables.get(tableName))
            .map(table -> table.byteLsiSize(lsiIndexName))
            .orElseThrow();
    }

    @Override
    public long getGSIByteSize(final String tableName,
                               final String gsiIndexName) {
        return Optional
            .ofNullable(tables.get(tableName))
            .map(table -> table.byteGsiSize(gsiIndexName))
            .orElseThrow();
    }

    @Override
    public ReentrantReadWriteLock getLockForTable(final String tableName) {
        return Optional
            .ofNullable(tables.get(tableName))
            .map(table -> table.getReentrantLock())
            .orElse(writeLock);
    }

    @Override
    public Map<String, List<GlobalSecondaryIndexDescription>> getGSIsByStatusFromAllTables(final IndexStatus indexStatus,
                                                                                           final Boolean backfilling) {
        return Map.of();
    }

    @Override
    public Map<String, TableSchemaInfo> fetchAllTablesWithTimeToLiveEnabled() {
        return tables.values().stream().filter(table -> table.hasTTLEnabled()).collect(
            Collectors.toMap(table-> table.getName(), table-> table.getSchemaInfo()));
    }

    @Override
    public void createGSIColumns(String tableName, String indexName) {
    }

    @Override
    public void backfillGSI(String tableName, String indexName) {
    }

    @Override
    public void deleteGSI(String tableName, String indexName) {
    }

    @Override
    public int numberOfSubscriberWideInflightOnlineCreateIndexesOperations() {
        return 0;
    }

    @Override
    public void optimizeDBBeforeStartup() {
    }

    @Override
    public List<Record> getStreamRecords(final Integer limit,
                                         final ShardIterator shardIterator) {
        return List.of();
    }

    @Override
    public Long getLatestSequenceNumberForShard(final String shardIds) {
        return 0L;
    }

    @Override
    public long getDeletionDateTimeForShard(final String shardId) {
        return 0;
    }

    @Override
    public Long getEarliestNonExpiredSequenceNumberForShard(final String shardId) {
        return 0L;
    }

    @Override
    public Long getSequenceNumberStartForShard(final String sshardId) {
        return 0L;
    }

    @Override
    public boolean shardIsNotExpired(final String shardId) {
        return true;
    }

    @Override
    public void dilateEventTimes(final long millis) {

    }

    @Override
    public void findAndRolloverActiveShards(final String tableName,
                                            final long shardAge) {
    }

    @Override
    public byte[] beginTransaction(final String transactionId) {
        return new byte[0];
    }

    @Override
    public void commitTransaction(final String transactionId,
                                  final byte[] signature) {
    }

    @Override
    public void rollbackTransaction() {
    }
}
