package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/PuerkitoBio/goquery"
)

type Item struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

var pictures = make(map[string]struct{})

func main() {
	config, err := config.LoadDefaultConfig(context.TODO(), func(o *config.LoadOptions) error {
		return nil
	})
	if err != nil {
		panic(err)
	}

	now := time.Now()

	process(config)

	fmt.Println("Time taken: ", time.Since(now))
	fmt.Println("Number of pictures: ", len(pictures))

	writeResults(pictures)
}

func process(config aws.Config) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var resultCh = make(chan map[string]interface{}, 100)

	client := dynamodb.NewFromConfig(config)
	table := "edl-cc-goal-content-prod"

	go func() {
		defer close(resultCh)
		var lastKey map[string]types.AttributeValue
		var scannedItems int

		for {
			result, err := client.Scan(context.Background(), &dynamodb.ScanInput{
				TableName:         aws.String(table),
				ExclusiveStartKey: lastKey,
			})
			if err != nil {
				log.Fatal("scan failed", err)
			}

			scannedItems += len(result.Items)

			fmt.Println("Scanned items:", scannedItems)

			for _, i := range result.Items {
				wg.Add(1)
				go func(item map[string]types.AttributeValue) {
					defer wg.Done()
					var itemMap map[string]interface{}
					if err := attributevalue.UnmarshalMap(item, &itemMap); err != nil {
						log.Fatal("unmarshal failed", err)
					}
					resultCh <- itemMap
				}(i)
			}

			if result.LastEvaluatedKey == nil {
				break
			}

			lastKey = result.LastEvaluatedKey
		}

		wg.Wait()
	}()

	for item := range resultCh {
		traverseEntry(item, &mu)
	}
}

func traverseEntry(entry map[string]interface{}, mu *sync.Mutex) {
	for key, value := range entry {
		if value == nil {
			continue
		}

		switch key {
		case "htmlBody":
			if htmlBody, ok := value.(string); ok {
				parseHtmlBody(htmlBody)
			}
		case "darkModeLogo", "lightModeLogo", "picture":
			if pictureMap, ok := value.(map[string]interface{}); ok {
				if id, ok := pictureMap["id"].(string); ok {
					mu.Lock()
					pictures[id] = struct{}{}
					mu.Unlock()
				}
			}
		default:
			if valueMap, ok := value.(map[string]interface{}); ok {
				traverseEntry(valueMap, mu)
			} else if valueArray, ok := value.([]interface{}); ok {
				for _, item := range valueArray {
					if itemMap, ok := item.(map[string]interface{}); ok {
						traverseEntry(itemMap, mu)
					}
				}
			}
		}
	}
}

func parseHtmlBody(htmlBody string) {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlBody))
	if err != nil {
		fmt.Println("Error loading HTML body:", err)
		return
	}

	doc.Find("script[type=\"application/json\"]").Each(func(i int, s *goquery.Selection) {
		var data map[string]interface{}
		err := json.Unmarshal([]byte(s.Text()), &data)
		if err != nil {
			fmt.Println("Error parsing json:", err)
			return
		}

		if dataType, ok := data["type"].(string); ok && dataType == "PICTURE" {
			if picture, ok := data["picture"].(map[string]interface{}); ok {
				if id, ok := picture["id"].(string); ok {
					pictures[id] = struct{}{}
				}
			}
		}
	})
}

func writeResults(pictures map[string]struct{}) {
	pictureList := make([]string, 0, len(pictures))
	for pic := range pictures {
		pictureList = append(pictureList, pic)
	}
	output, err := json.MarshalIndent(pictureList, "", "  ")
	if err != nil {
		fmt.Println("Error marshalling pictures:", err)
		return
	}

	filePath := "pictures.json"
	err = os.WriteFile(filePath, output, 0644)
	if err != nil {
		fmt.Println("Error writing to file:", err)
	}
}
