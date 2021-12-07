aws dynamodb list-tables --endpoint-url http://localhost:8000

aws dynamodb create-table \
    --table-name MusicCollection \
    --attribute-definitions AttributeName=Artist,AttributeType=S AttributeName=SongTitle,AttributeType=S AttributeName=AlbumTitle,AttributeType=S \
    --key-schema AttributeName=Artist,KeyType=HASH AttributeName=SongTitle,KeyType=RANGE \
    --provisioned-throughput ReadCapacityUnits=10,WriteCapacityUnits=5 \
    --endpoint-url http://localhost:8000 \
    --local-secondary-indexes \
        "[
            {
                \"IndexName\": \"AlbumTitleIndex\",
                \"KeySchema\": [
                    {\"AttributeName\": \"Artist\",\"KeyType\":\"HASH\"},
                    {\"AttributeName\": \"AlbumTitle\",\"KeyType\":\"RANGE\"}
                ],
                \"Projection\": {
                    \"ProjectionType\": \"INCLUDE\",
                    \"NonKeyAttributes\": [\"Genre\", \"Year\"]
                }
            }
        ]"


aws dynamodb describe-table \
  --table-name MusicCollection \
  --endpoint-url http://localhost:8000

aws dynamodb batch-write-item --request-items file://./items.json \
    --endpoint-url http://localhost:8000 \
    --return-consumed-capacity INDEXES \
    --return-item-collection-metrics SIZE

aws dynamodb scan --table-name MusicCollection \
    --endpoint-url http://localhost:8000

aws dynamodb scan --table-name MusicCollection \
    --endpoint-url http://localhost:8000  --index-name  AlbumTitleIndex
