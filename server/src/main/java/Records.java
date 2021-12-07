import com.amazonaws.services.dynamodbv2.local.shared.access.QueryResultInfo;
import com.amazonaws.services.dynamodbv2.local.shared.exceptions.LocalDBAccessException;
import com.amazonaws.services.dynamodbv2.local.shared.exceptions.LocalDBAccessExceptionType;
import com.amazonaws.services.dynamodbv2.local.shared.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.local.shared.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Records {
    private final ConcurrentSkipListMap<AttributeValue, ConcurrentSkipListMap<AttributeValue, Map<String, AttributeValue>>> records;
    private final Function<Map<String, AttributeValue>, AttributeValue> extractHash;
    private final Function<Map<String, AttributeValue>, AttributeValue> extractRange;
    private final String hashKeyName;
    private final String rangeKeyName;

    public Records(
        final String hashKeyName,
        final String rangeKeyName
    ) {
        this.hashKeyName = hashKeyName;
        this.rangeKeyName = rangeKeyName;
        this.extractHash = (map) -> map.get(hashKeyName);
        this.extractRange = (map) -> map.get(rangeKeyName);
        records = new ConcurrentSkipListMap<>();
    }

    public void put(Map<String, AttributeValue> record) {
        final AttributeValue hashKey = extractHash.apply(record);
        final AttributeValue rangeKey = extractRange.apply(record);


        var rangeMap = records.get(hashKey);
        if (rangeMap == null) {
            rangeMap = new ConcurrentSkipListMap<>();
            records.put(hashKey, rangeMap);
        }

        rangeMap.put(rangeKey, record);
    }

    public Map<String, AttributeValue> getRecord(final Map<String, AttributeValue> primaryKey) {
        final AttributeValue hashKey = extractHash.apply(primaryKey);
        final AttributeValue rangeKey = extractRange.apply(primaryKey);

        if (hashKey == null || rangeKey == null) {
            return Map.of();
        }

        final Map<AttributeValue, Map<String, AttributeValue>> rangeMap = records.get(hashKey);
        if (rangeMap == null) {
            return Map.of();
        }

        final Map<String, AttributeValue> recordMap = rangeMap.get(rangeKey);
        if (recordMap == null) {
            return Map.of();
        }

        return recordMap;
    }

    public boolean deleteRecord(final Map<String, AttributeValue> primaryKey) {
        final AttributeValue hashKey = extractHash.apply(primaryKey);
        final AttributeValue rangeKey = extractRange.apply(primaryKey);

        if (hashKey == null || rangeKey == null) {
            return false;
        }

        final Map<AttributeValue, Map<String, AttributeValue>> rangeMap = records.get(hashKey);
        if (rangeMap == null) {
            return false;
        }

        final Map<String, AttributeValue> recordMap = rangeMap.remove(rangeKey);
        return recordMap != null;
    }

    public QueryResultInfo queryRecords(final Map<String, Condition> conditions,
                                        final Map<String, AttributeValue> exclusiveStartKey,
                                        final Long limit,
                                        final String tableRangeKey) {
        final Optional<AttributeValue> hk = Optional.ofNullable(exclusiveStartKey).map(k ->extractHash.apply(k));
        final Optional<AttributeValue> rk = Optional.ofNullable(exclusiveStartKey).map(k ->extractRange.apply(k));

        final var recordsToTraverse = hk
            .map(key -> records.tailMap(key, true))
            .orElse(records);

        final NavigableSet<AttributeValue> hashKeysToTraverse = recordsToTraverse.keySet();

        AtomicReference<Map<String, AttributeValue>> lastProceesedContent = new AtomicReference<>();

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

                lastProceesedContent.set(content);

                boolean result = true;

                if (conditions == null) {
                    return result;
                }

                if (conditions.containsKey(hashKeyName)) {
                    result = result && predicate(hashKeyValue,
                        conditions.get(hashKeyName));
                }

                if (conditions.containsKey(rangeKeyName)) {
                    result = result && predicate(rangeKeyValue,
                        conditions.get(rangeKeyName));
                }

                result = result && content.entrySet()
                    .stream()
                    .filter(e -> conditions.containsKey(e.getKey()))
                    .map(e -> predicate(e.getValue(), conditions.get(e.getKey())))
                    .reduce(true, (acc, value) -> acc && value)
                ;

                return result;
            })
            .limit(limit == null ? 1000 : limit < 0 ? 1000: limit)
            .map(record -> {
                Map<String, AttributeValue> map = new HashMap<>(record.getValue().getValue());
                map.put(hashKeyName, record.getKey());
                map.put(rangeKeyName, record.getValue().getKey());
                return map;
            })
            .collect(Collectors.toList());

        Map<String, AttributeValue> lastProcessed = new HashMap<>();
        final Map<String, AttributeValue> content = lastProceesedContent.get();

        if (content != null) {
            lastProcessed = new HashMap<>();
            lastProcessed.put(hashKeyName, content.get(hashKeyName));
            lastProcessed.put(rangeKeyName, content.get(rangeKeyName));
            lastProcessed.put(tableRangeKey, content.get(tableRangeKey));
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
}
