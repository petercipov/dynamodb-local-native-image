import com.amazonaws.services.dynamodbv2.local.shared.access.QueryResultInfo;
import com.amazonaws.services.dynamodbv2.local.shared.access.TableInfo;
import com.amazonaws.services.dynamodbv2.local.shared.access.sqlite.TableSchemaInfo;
import com.amazonaws.services.dynamodbv2.local.shared.exceptions.LocalDBAccessException;
import com.amazonaws.services.dynamodbv2.local.shared.exceptions.LocalDBAccessExceptionType;
import com.amazonaws.services.dynamodbv2.local.shared.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.local.shared.model.Condition;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.IndexStatus;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;

import java.util.AbstractMap;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class Table {
    private final String tableName;
    private final AttributeDefinition hashKeyDef;
    private final AttributeDefinition rangeKeyDef;
    private final Map<String, AttributeDefinition> allAttributes;
    private final Map<String, LocalSecondaryIndex> lsiIndexes;
    private final Map<String, GlobalSecondaryIndex> gsiIndexes;
    private final ProvisionedThroughput throughput;
    private final BillingMode billingMode;
    private final StreamSpecification streamSpecification;
    private Optional<String> timeToLiveAttributeName;
    private ReentrantReadWriteLock writeLock = new ReentrantReadWriteLock();

    //hashKey, rangeKey
    private final ConcurrentSkipListMap<AttributeValue, ConcurrentSkipListMap<AttributeValue, Map<String, AttributeValue>>> records;

    public Table(final String tableName,
                 final AttributeDefinition hashKey,
                 final AttributeDefinition rangeKey,
                 final List<AttributeDefinition> allAttributes,
                 final List<LocalSecondaryIndex> lsiIndexes,
                 final List<GlobalSecondaryIndex> gsiIndexes,
                 final ProvisionedThroughput throughput,
                 final BillingMode billingMode,
                 final StreamSpecification streamSpecification) {

        this.tableName = tableName;
        this.hashKeyDef = hashKey;
        this.rangeKeyDef = rangeKey;
        this.allAttributes = allAttributes.stream().collect(Collectors.toMap(def -> def.getAttributeName(), def -> def));
        this.lsiIndexes =
            (lsiIndexes == null ? List.<LocalSecondaryIndex>of() : lsiIndexes)
                .stream()
                .collect(Collectors.toMap(def -> def.getIndexName(), def -> def));
        this.gsiIndexes =
            (gsiIndexes == null ? List.<GlobalSecondaryIndex>of() : gsiIndexes)
                .stream()
                .collect(Collectors.toMap(def -> def.getIndexName(), def -> def));
        this.throughput = throughput;
        this.billingMode = billingMode;
        this.streamSpecification = streamSpecification;

        this.timeToLiveAttributeName = Optional.empty();
        this.records = new ConcurrentSkipListMap<>();
    }

    public void update(final List<AttributeDefinition> updatedAttributeDefinitions,
                       final List<GlobalSecondaryIndexDescription> gsiIndexes) {

        updatedAttributeDefinitions.forEach(def -> {
            allAttributes.put(def.getAttributeName(), def);
        });

        gsiIndexes
            .stream()
            .map(x -> {
                GlobalSecondaryIndex gsi = new GlobalSecondaryIndex();
                gsi.setIndexName(x.getIndexName());
                gsi.setKeySchema(x.getKeySchema());
                gsi.setProjection(x.getProjection());

                final ProvisionedThroughput throughput = new ProvisionedThroughput();
                throughput.setReadCapacityUnits(x.getProvisionedThroughput().getReadCapacityUnits());
                throughput.setWriteCapacityUnits(x.getProvisionedThroughput().getWriteCapacityUnits());

                gsi.setProvisionedThroughput(throughput);
                return gsi;
            })
            .forEach(def -> {
            this.gsiIndexes.put(def.getIndexName(), def);
        });

    }

    public void update(final String timeToLiveAttributeName) {
        this.timeToLiveAttributeName = Optional.ofNullable(timeToLiveAttributeName);
    }

    public Map<String, AttributeValue> getRecord(final Map<String, AttributeValue> primaryKey) {
        final AttributeValue hk = primaryKey.get(hashKeyDef.getAttributeName());
        final AttributeValue rk = primaryKey.get(rangeKeyDef.getAttributeName());

        if (hk == null || rk == null) {
            return Map.of();
        }

        final Map<AttributeValue, Map<String, AttributeValue>> rangeMap = records.get(hk);
        if (rangeMap == null) {
            return Map.of();
        }

        final Map<String, AttributeValue> recordMap = rangeMap.get(rk);
        if (recordMap == null) {
            return Map.of();
        }

        return recordMap;
    }

    public boolean deleteRecord(final Map<String, AttributeValue> primaryKey, final boolean isSystemDelete) {
        final AttributeValue hk = primaryKey.get(hashKeyDef.getAttributeName());
        final AttributeValue rk = primaryKey.get(rangeKeyDef.getAttributeName());

        if (hk == null || rk == null) {
            return false;
        }

        final Map<AttributeValue, Map<String, AttributeValue>> rangeMap = records.get(hk);
        if (rangeMap == null) {
            return false;
        }

        final Map<String, AttributeValue> recordMap = rangeMap.remove(rk);
        return recordMap != null;
    }

    public void putRecord(final AttributeValue hashKey,
                          final AttributeValue rangeKey,
                          final Map<String, AttributeValue> record) {

        var rangeMap = records.get(hashKey);
        if (rangeMap == null) {
            rangeMap = new ConcurrentSkipListMap<>();
            records.put(hashKey, rangeMap);
        }

        rangeMap.put(rangeKey, record);
    }

    public QueryResultInfo queryRecords(final String indexName,
                                        final Map<String, Condition> conditions,
                                        final Map<String, AttributeValue> exclusiveStartKey,
                                        final Long limit,
                                        final boolean ascending,
                                        final byte[] beginHash,
                                        final byte[] endHash,
                                        final boolean isScan,
                                        final boolean isGSIIndex) {
        final Optional<AttributeValue> hk = Optional.ofNullable(exclusiveStartKey.get(hashKeyDef.getAttributeName()));
        final Optional<AttributeValue> rk = Optional.ofNullable(exclusiveStartKey.get(rangeKeyDef.getAttributeName()));

        final var recordsToTraverse = hk
            .map(key -> records.tailMap(key, true))
            .orElse(records);

        final NavigableSet<AttributeValue> hashKeysToTraverse = recordsToTraverse.keySet();

        AtomicReference<AttributeValue> lastProceesedHash = new AtomicReference<>();
        AtomicReference<AttributeValue> lastProcessedRange = new AtomicReference<>();

        final List<Map<String, AttributeValue>> scannedRecords = hashKeysToTraverse.stream()
            .flatMap(hashKey -> {
                final boolean is_hk_exclusive = hk.map(hashKey::equals).orElse(false);
                final var rangeMap = recordsToTraverse.get(hashKey);

                if (is_hk_exclusive) {
                    final var rangesToTraverse = rk
                        .map(key -> rangeMap.tailMap(key, false))
                        .orElse(rangeMap);
                    return rangesToTraverse.entrySet().stream().map(e -> new AbstractMap.SimpleEntry<>(hashKey, e));
                } else {
                    return rangeMap.entrySet().stream().map(e -> new AbstractMap.SimpleEntry<>(hashKey, e));
                }

            })
            .filter(entry -> {
                final AttributeValue hashKeyValue = entry.getKey();
                final AttributeValue rangeKeyValue = entry.getValue().getKey();
                final var content = entry.getValue().getValue();

                lastProceesedHash.set(hashKeyValue);
                lastProcessedRange.set(rangeKeyValue);

                boolean result = true;

                if (conditions.containsKey(hashKeyDef.getAttributeName())) {
                    result = result && predicate(hashKeyValue,
                        conditions.get(hashKeyDef.getAttributeName()));
                }

                if (conditions.containsKey(rangeKeyDef.getAttributeName())) {
                    result = result && predicate(rangeKeyValue,
                        conditions.get(rangeKeyDef.getAttributeName()));
                }

                result = result && content.entrySet()
                    .stream()
                    .filter(e -> conditions.containsKey(e.getKey()))
                    .map(e -> predicate(e.getValue(), conditions.get(e.getKey())))
                    .reduce(true, (acc, value) -> acc && value)
                ;

                return result;
            })
            .limit(limit == null ? 1000 : limit)
            .map(record -> {
                Map<String, AttributeValue> map = new HashMap<>(record.getValue().getValue());
                map.put(hashKeyDef.getAttributeName(), record.getKey());
                map.put(rangeKeyDef.getAttributeName(), record.getValue().getKey());
                return map;
            })
            .collect(Collectors.toList());

        Map<String, AttributeValue> lastProcessed = new HashMap<>();
        if (lastProceesedHash.get() != null && lastProcessedRange.get() != null) {
            lastProcessed.put(hashKeyDef.getAttributeName(), lastProceesedHash.get());
            lastProcessed.put(rangeKeyDef.getAttributeName(), lastProcessedRange.get());
        }
        return new QueryResultInfo(scannedRecords, lastProcessed);
    }

    private boolean predicate(AttributeValue value, Condition condition) {

        ComparisonOperator comparisonOperator = ComparisonOperator.fromValue(condition.getComparisonOperator());
        final AttributeValue conditionValue = condition.getAttributeValueList().get(0);

        switch (comparisonOperator) {
            case EQ:
                return value.equals(conditionValue);
            case LT:
                return value.lessThan(conditionValue);
            case GT:
                return value.greaterThan(conditionValue);
            case LE:
                return value.equals(conditionValue) || value.lessThan(conditionValue) ;
            case GE:
                return value.equals(conditionValue) || value.greaterThan(conditionValue) ;
            case BEGINS_WITH:
                return value.getSValue().startsWith(conditionValue.getSValue());
            case BETWEEN:
                return
                    (value.equals(conditionValue) || value.lessThan(conditionValue)) &&
                    (value.equals(conditionValue) || value.greaterThan(conditionValue));
            default:
                throw new LocalDBAccessException(LocalDBAccessExceptionType.VALIDATION_EXCEPTION, "Unsupported comparison operator for query: " + comparisonOperator);
        }
    }

    public long itemCount() {
        return 0;
    }

    public long itemLsiCount(final String lsiIndexName) {
        return 0;
    }


    public long itemGsiCount(final String lsiIndexName) {
        return 0;
    }

    public TableInfo getInfo() {
        return new TableInfo(
            tableName,
            hashKeyDef,
            rangeKeyDef,
            allAttributes.values().stream().collect(Collectors.toList()),
            lsiIndexes.values().stream().collect(Collectors.toList()),
            gsiIndexes.values().stream()
                .map(x -> new GlobalSecondaryIndexDescription()
                    .withIndexName(x.getIndexName())
                    .withBackfilling(false)
                    .withIndexArn("ARN:")
                    .withIndexStatus(IndexStatus.ACTIVE)
                    .withIndexSizeBytes(0L)
                    .withItemCount(0L)
                    .withKeySchema(x.getKeySchema())
                    .withProjection(x.getProjection())
                    .withProvisionedThroughput(new ProvisionedThroughputDescription()
                        .withReadCapacityUnits(x.getProvisionedThroughput().getReadCapacityUnits())
                        .withWriteCapacityUnits(x.getProvisionedThroughput().getWriteCapacityUnits())
                        .withNumberOfDecreasesToday(0L)
                        .withLastDecreaseDateTime(new Date())
                        .withLastIncreaseDateTime(new Date())
                    )
                )
                .collect(Collectors.toList()),
            throughput,
            billingMode,
            streamSpecification,
            0
        );
    }

    public long byteSize() {
        return 0;
    }

    public long byteLsiSize(final String lsiIndexName) {
        return 0;
    }

    public long byteGsiSize(final String gsiIndexName) {
        return 0;
    }

    public ReentrantReadWriteLock getReentrantLock() {
        return writeLock;
    }

    public boolean hasTTLEnabled() {
        return false;
    }

    public TableSchemaInfo getSchemaInfo() {
        return null;
    }

    public String getName() {
        return tableName;
    }
}
