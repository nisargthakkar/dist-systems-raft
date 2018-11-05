package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"bufio"
	"strings"
	"strconv"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/lab-2-raft-nisargthakkar/pb"
)

func usage() {
	fmt.Printf("Usage %s <endpoint1> <endpoint2> ...\n", os.Args[0])
	flag.PrintDefaults()
}

func connectAll(endpoints []string) []pb.KvStoreClient {
	clients := make([]pb.KvStoreClient, flag.NArg())
	for clientId, endpoint := range endpoints {
		log.Printf("Connecting to %v", endpoint)
		// Connect to the server. We use WithInsecure since we do not configure https in this class.
		conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
		//Ensure connection did not fail.
		if err != nil {
			log.Fatalf("Failed to dial GRPC server %v", err)
		}
		log.Printf("Connected")
		// Create a KvStore client
		clients[clientId] = pb.NewKvStoreClient(conn)
	}
	return clients
}

func main() {

	// Take endpoint as input
	flag.Usage = usage
	flag.Parse()
	// If there is no endpoint fail
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(1)
	}
	clients := make([]pb.KvStoreClient, flag.NArg())

	reader := bufio.NewReader(os.Stdin)
	clients = connectAll(flag.Args())
	
	log.Printf("Start commanding")
	for {
		text, _ := reader.ReadString('\n')
		params := strings.Split(strings.TrimSpace(text), " ")

		switch params[0] {
		case "connectall":
			clients = connectAll(flag.Args())
			continue
		}

		clientId, _ := strconv.ParseInt(params[0], 10, 0)
		switch params[1] {
		case "connect":
			endpoint := flag.Args()[clientId]
			log.Printf("Connecting to %v", endpoint)
			// Connect to the server. We use WithInsecure since we do not configure https in this class.
			conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
			//Ensure connection did not fail.
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			// Create a KvStore client
			kvc := pb.NewKvStoreClient(conn)
			clients[clientId] = kvc
		case "clear":
			// Clear KVC
			res, err := clients[clientId].Clear(context.Background(), &pb.Empty{})
			if err != nil {
				log.Fatalf("Could not clear")
			}
			if res.GetRedirect() != nil {
				log.Printf("Got response to redirect to: \"%v\"", res.GetRedirect().Server)
				continue
			}
			log.Printf("Got response %v", res)
		case "get":
			// Request value for hello
			req := &pb.Key{Key: params[2]}
			res, err := clients[clientId].Get(context.Background(), req)
			if err != nil {
				log.Fatalf("Request error %v", err)
			}

			if res.GetRedirect() != nil {
				log.Printf("Got response to redirect to: \"%v\"", res.GetRedirect().Server)
				continue
			}

			log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
			if res.GetKv().Key != params[2] {
				log.Printf("Get returned the wrong response")
			}
		case "set":
			// Put setting hello -> 1
			putReq := &pb.KeyValue{Key: params[2], Value: params[3]}
			res, err := clients[clientId].Set(context.Background(), putReq)
			if err != nil {
				log.Fatalf("Put error")
			}

			if res.GetRedirect() != nil {
				log.Printf("Got response to redirect to: \"%v\"", res.GetRedirect().Server)
				continue
			}

			log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
			if res.GetKv().Key != params[2] || res.GetKv().Value != params[3] {
				log.Printf("Put returned the wrong response")
			}
		case "cas":
			casReq := &pb.CASArg{Kv: &pb.KeyValue{Key: params[2], Value: params[3]}, Value: &pb.Value{Value: params[4]}}
			res, err := clients[clientId].CAS(context.Background(), casReq)
			if err != nil {
				log.Fatalf("Request error %v", err)
			}

			if res.GetRedirect() != nil {
				log.Printf("Got response to redirect to: \"%v\"", res.GetRedirect().Server)
				continue
			}

			log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
			if res.GetKv().Key != params[2] || res.GetKv().Value != params[4] {
				log.Printf("Get returned the wrong response")
			}
		}
	}
}
