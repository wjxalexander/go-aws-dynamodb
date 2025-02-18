package database

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/wjxalexander/go-aws-dynamodb/types"
)

// Get table items from JSON file
func getItems() []types.Item {
	raw, err := os.ReadFile("../movie_data.json")
	if err != nil {
		log.Fatalf("Got error reading file: %s", err)
	}

	var items []types.Item
	json.Unmarshal(raw, &items) // Direction: JSON → Go struct
	return items
}

const (
	BATCH_SIZE = 25
)

func (d DynamoDBClient) BatchInsertMovies() error {
	movies := getItems()
	for i := 0; i < len(movies); i += BATCH_SIZE {
		end := i + BATCH_SIZE
		if end > len(movies) {
			end = len(movies)
		}

		batch := movies[i:end]

		// Create write requests for the batch
		var writeRequests []*dynamodb.WriteRequest
		for _, movie := range batch {
			// Convert movie to DynamoDB attribute values
			av, err := dynamodbattribute.MarshalMap(movie) // Go struct → DynamoDB attributes
			if err != nil {
				return fmt.Errorf("error marshalling movie: %v", err)
			}

			// Create PutRequest for the item
			writeRequests = append(writeRequests, &dynamodb.WriteRequest{
				PutRequest: &dynamodb.PutRequest{
					Item: av,
				},
			})
		}
		input := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				TABLE_NAME: writeRequests,
			},
		}
		for {
			result, err := d.database.BatchWriteItem(input)
			if err != nil {
				return fmt.Errorf("error in BatchWriteItem: %v", err)
			}

			// Check if there are any unprocessed items
			if len(result.UnprocessedItems) == 0 {
				break
			}

			// Retry with unprocessed items
			input.RequestItems = result.UnprocessedItems
			time.Sleep(time.Second) // Add a small delay before retrying
		}
	}
	return nil
}
