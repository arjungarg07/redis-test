package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/redis/go-redis/v9"
	"log"
	pb "module-path/schema/types"
	"time"
)

func main() {
	// Create an instance of the protobuf message
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	post := &pb.PostFeatureStaticResponse{
		ContentType:       0,
		TagId:             "17598940",
		CreatorId:         "1710570516",
		Language:          "Hindi",
		Genre:             "N/A",
		CreatorBadge:      "",
		L0Taxonomy:        "",
		L1Taxonomy:        "",
		L2Taxonomy:        "",
		L3Taxonomy:        "",
		L4Taxonomy:        "",
		CreatedOn:         "1698728604",
		CommentOff:        true,
		PostShareDisabled: false,
		Height:            "1920",
		Width:             "1080",
		L0Topic:           "",
		L1Topic:           "",
		L2Topic:           "",
		L1TopicV2:         "",
		Duration:          17.159,
		CreatorIp:         "183702262197479816",
		CreatorCity:       "bhiwani",
		CreatorState:      "haryana",
		Predictedprob:     0.32963661752448825,
		Predictedtopic:    "food",
		HybridTopic:       "",
		CreatorGender:     "",
		Badge:             "UNK",
		CreatorType:       "unverified",
	}

	// Serializing the protobuf message
	data, err := proto.Marshal(post)
	if err != nil {
		log.Fatal("Error marshalling protobuf:", err)
	}

	redisKey := "ps:proto"
	ctx := context.Background()
	err = client.Set(ctx, redisKey, data, 1*time.Second).Err()
	if err != nil {
		log.Fatal("Error setting data in Redis:", err)
	}

	size, err := client.MemoryUsage(ctx, redisKey).Result()
	println("Memory usage of protobuf", size)
	if err != nil {
		log.Fatal("Error getting size from Redis:", err)
	}

	// input string
	inputString := "video^17598940^1710570516^Hindi^N/A^^^^^^^1698728604^0^0^1920^1080^^^^Food^17.159^183702262197479816^bhiwani^haryana^0.32963661752448825^food^^UNK^unverified^UNK"
	inputBytes := []byte(inputString)
	// Gzip compression
	compressedBytes, err := gzipCompress(inputBytes)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	// Output sizes
	fmt.Printf("Original String Size: %d bytes\n", len(inputBytes))
	fmt.Printf("Compressed String Size: %d bytes\n", len(compressedBytes))

	redisKey = "ps:compressed_string"
	err = client.Set(ctx, redisKey, data, 0).Err()
	if err != nil {
		log.Fatal("Error setting data in Redis:", err)
	}

	// Get the size of stored data
	size, err = client.MemoryUsage(ctx, redisKey).Result()
	println("Memory usage of Compressed string%s", size)
	if err != nil {
		log.Fatal("Error getting size from Redis:", err)
	}
}

func gzipCompress(input []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)

	_, err := writer.Write(input)
	if err != nil {
		return nil, err
	}

	err = writer.Close()
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
