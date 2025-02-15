package database

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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
