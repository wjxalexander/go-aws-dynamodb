package database

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/google/uuid"
	"github.com/wjxalexander/go-aws-dynamodb/types"
)

type DynamoDBClient struct {
	database *dynamodb.DynamoDB
}

const (
	TABLE_NAME = "ProvisionedThroughput"
)

func NewDynamoDBClient() DynamoDBClient {
	dbSession := session.Must(session.NewSession())
	db := dynamodb.New(dbSession)
	return DynamoDBClient{
		database: db,
	}
}

func (u DynamoDBClient) CreateTable() error {
	// create a table
	input := &dynamodb.CreateTableInput{
		// Defines the attributes that will be used in key schemas or indexes
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("Year"),
				AttributeType: aws.String("N"), // Number
			},
			{
				AttributeName: aws.String("Title"),
				AttributeType: aws.String("S"),
			},
		},
		// Defines the structure of the primary key
		// Can only contain attributes that are defined in AttributeDefinitions
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("Year"),
				KeyType:       aws.String("HASH"), // Must have a partition key (HASH)
			},
			{
				AttributeName: aws.String("Title"),
				KeyType:       aws.String("RANGE"), // Can optionally have a sort key (RANGE)
			},
		},
		// defines the maximum amount of read and write capacity units a table can handle per second
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
		TableName: aws.String(TABLE_NAME),
	}
	_, err := u.database.CreateTable(input)
	if err != nil {
		return fmt.Errorf("error creating table: %v", err)
	}
	fmt.Println("Created the table", TABLE_NAME)
	return nil
}

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

// func main() {
// 	dbClient := NewDynamoDBClient()
// 	err := dbClient.CreateTable()
// 	if err != nil {
// 		log.Fatalf("Failed to create table: %v", err)
// 	}
// 	fmt.Println("Table created successfully!")
// }
