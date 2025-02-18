package database

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/google/uuid"
	"github.com/wjxalexander/go-aws-dynamodb/types"
)

func (u DynamoDBClient) InsertItem(item types.Item) error {
	item.Id = uuid.New().String()
	av, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		return fmt.Errorf("error marshalling item: %v", err)
	}
	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(TABLE_NAME),
		// Optional: Add condition to prevent overwriting existing items
		ConditionExpression: aws.String("attribute_not_exists(ID)"),
	}
	_, err = u.database.PutItem(input)
	if err != nil {
		// Check for conditional check failure
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
			return fmt.Errorf("item with ID already exists")
		}
		return fmt.Errorf("error putting item: %v", err)
	}

	return nil
}
