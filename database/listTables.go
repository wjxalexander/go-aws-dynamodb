package database

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (u DynamoDBClient) ListTables() error {
	input := &dynamodb.ListTablesInput{}
	for {
		result, err := u.database.ListTables(input)
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case dynamodb.ErrCodeInternalServerError:
					fmt.Println(dynamodb.ErrCodeInternalServerError, aerr.Error())
				default:
					fmt.Println(aerr.Error())
				}
			} else {
				// Print the error, cast err to awserr.Error to get the Code and
				// Message from an error.
				fmt.Println(err.Error())
			}
			return err
		}
		// str := "hello"      // Regular string
		// ptr := &str         // & creates a pointer to str
		// value := *ptr       // * dereferences ptr to get "hello"

		for _, name := range result.TableNames {
			fmt.Printf("name: %v\n", name)   // Prints pointer address
			fmt.Printf("*name: %v\n", *name) // Prints actual table name
			fmt.Printf("&name: %v\n", &name) // Prints pointer to pointer
		}

		input.ExclusiveStartTableName = result.LastEvaluatedTableName

		if result.LastEvaluatedTableName == nil {
			break
		}

	}
	return nil
}
