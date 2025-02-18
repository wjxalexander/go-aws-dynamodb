package database

import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/wjxalexander/go-aws-dynamodb/types"
)

func (d DynamoDBClient) ReadItem(movieName string, movieYear string) ([]types.Item, error) {
	var item []types.Item

	result, err := d.database.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(TABLE_NAME),
		Key: map[string]*dynamodb.AttributeValue{
			"Year": {
				S: aws.String(movieYear),
			},
			"Title": {
				S: aws.String(movieName),
			},
		},
	})
	if err != nil {
		log.Fatalf("Got error calling GetItem: %s", err)
	}
	marshalErr := dynamodbattribute.UnmarshalMap(result.Item, &item)
	if marshalErr != nil {
		return item, marshalErr
	}
	return item, nil
}
