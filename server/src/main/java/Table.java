import com.amazonaws.services.dynamodbv2.local.shared.access.QueryResultInfo;
import com.amazonaws.services.dynamodbv2.local.shared.access.TableInfo;
import com.amazonaws.services.dynamodbv2.local.shared.access.sqlite.TableSchemaInfo;
import com.amazonaws.services.dynamodbv2.local.shared.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.local.shared.model.Condition;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.BillingMode;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.IndexStatus;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;

import java.util.AbstractMap;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
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

    private final Records tableRecords;
    private final Map<String, Records> secondaryIndexRecords;

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
        this.tableRecords = new Records(
            this.hashKeyDef.getAttributeName(),
            this.rangeKeyDef.getAttributeName());

        this.secondaryIndexRecords = lsiIndexes
            .stream()
            .map(def -> {
                final Optional<KeySchemaElement> hashKeySchema = def.getKeySchema()
                    .stream()
                    .filter(element -> element.getKeyType()
                        .equals(KeyType.HASH.toString()))
                    .findFirst();

                final Optional<KeySchemaElement> rangeKeySchema =
                    def.getKeySchema().stream().filter(element -> element.getKeyType()
                        .equals(KeyType.RANGE.toString())).findFirst();

                return new AbstractMap.SimpleEntry<String, Records>(
                    def.getIndexName(),
                    new Records(
                        hashKeySchema.get().getAttributeName(),
                        rangeKeySchema.get().getAttributeName())
                );
            })
            .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
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
        return this.tableRecords.getRecord(primaryKey);
    }

    public boolean deleteRecord(final Map<String, AttributeValue> primaryKey, final boolean isSystemDelete) {
        return tableRecords.deleteRecord(primaryKey);
    }

    public void putRecord(final AttributeValue hashKey,
                          final AttributeValue rangeKey,
                          final Map<String, AttributeValue> record) {

        this.tableRecords.put(record);
        this.secondaryIndexRecords.forEach((k, rec) -> {
            rec.put(record);
        });
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
        if (this.secondaryIndexRecords.containsKey(indexName)) {
            return this.secondaryIndexRecords.get(indexName).queryRecords(conditions, exclusiveStartKey, limit,
                rangeKeyDef.getAttributeName());
        }

        return this.tableRecords.queryRecords(conditions, exclusiveStartKey, limit, rangeKeyDef.getAttributeName());
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
