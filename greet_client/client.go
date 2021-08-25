package main

import (
	"context"
	"fmt"
	"go-grpc/greet/greetpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"io"
	"log"
	"time"
)

func main(){
	fmt.Println("Hello I am the client")
	// CA trust certificate
	certFile := "ssl/ca.crt"
	creds, sslErr := credentials.NewClientTLSFromFile(certFile, "")
	if sslErr != nil{
		log.Fatalf("Error while loading ca trust cert: %v", sslErr)
		return
	}

	opts := grpc.WithTransportCredentials(creds)
	cc, err := grpc.Dial("localhost:50051", opts)
	if err != nil {
		log.Fatalf("couldn't connect: %v\n", err)
	}

	c := greetpb.NewGreetServiceClient(cc)
	fmt.Printf("Created the client: %f", c)
	doUnary(c)
	//doServerStreaming(c)
	//doClientStreaming(c)
	//doBidiStreaming(c)
	//doUnaryWithDeadline(c, 5*time.Second)
	//doUnaryWithDeadline(c, 1*time.Second)
	defer cc.Close()
}

func doUnary(c greetpb.GreetServiceClient){
	fmt.Println("Starting a unary rpc")
	req := &greetpb.GreetRequest{
		Greeting: &greetpb.Greeting{
			FirstName: "Pavan",
			LastName: "Tikkani",
		},
	}
	ctx := context.WithValue(context.Background(),"key", "test-run")
	res, err := c.Greet(ctx, req)
	if err != nil{
		log.Fatalf("error while calling greet rpc: %v\n", err)
	}
	log.Printf("Response from Greet: %v", res.Result)
}

func doServerStreaming(c greetpb.GreetServiceClient){
	fmt.Println("Starting the streaming rpc...")
	req := &greetpb.GreetManyTimesRequest{
		Greeting: &greetpb.Greeting{
			FirstName: "Pavan",
			LastName: "Kumar",
		},
	}

	resStream, err := c.GreetManyTimes(context.Background(), req)
	if err != nil {
		log.Fatalf("error while calling greet many times RPC: %v", err)
	}
	for {
		msg, err := resStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("error while reading stream: %v", err)
		}
		log.Printf("Response from greet many times: %v", msg.GetResult())
	}
}

func doClientStreaming(c greetpb.GreetServiceClient){
	fmt.Println("Starting to do a Client streaming rpc")
	requests := []*greetpb.LongGreetRequest{
		&greetpb.LongGreetRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Pavan",
			},
		},
		&greetpb.LongGreetRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Roy",
			},
		},
		&greetpb.LongGreetRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Jason",
			},
		},
		&greetpb.LongGreetRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Logan",
			},
		},
		&greetpb.LongGreetRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Paul",
			},
		},

	}

	stream, err := c.LongGreet(context.Background())
	if err != nil {
		log.Fatalf("error while calling long greet: %v", err)
	}
	for _, req := range requests{
		fmt.Printf("Sending request: %v\n", req)
		stream.Send(req)
		time.Sleep(1000*time.Millisecond)
	}
	res, err := stream.CloseAndRecv()
	if err != nil{
		log.Fatalf("error while recieving response from long greet: %v", err)
	}
	fmt.Printf("Long Greet response: %v\n", res)
}

func doBidiStreaming(c greetpb.GreetServiceClient){
	fmt.Println("Starting BIDI streaming rpc")
//	Create a stream by invoking the client
	stream, err := c.GreetEveryone(context.Background())
	if err != nil {
		log.Fatalf("Error while creating the stream: %v", err)
	}
	fmt.Println("Starting to do a Client streaming rpc")
	requests := []*greetpb.GreetEveryoneRequest{
		&greetpb.GreetEveryoneRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Pavan",
			},
		},
		&greetpb.GreetEveryoneRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Roy",
			},
		},
		&greetpb.GreetEveryoneRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Jason",
			},
		},
		&greetpb.GreetEveryoneRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Logan",
			},
		},
		&greetpb.GreetEveryoneRequest{
			Greeting: &greetpb.Greeting{
				FirstName: "Paul",
			},
		},

	}
	waitc := make(chan struct{})
// We send a bunch of messages to server
// func to send a bunch of messages to the server
	go func() {
		for _,req := range requests{
			fmt.Printf("Sending Message: %v\n", req)
			stream.Send(req)
			time.Sleep(1000 * time.Millisecond)
		}
		stream.CloseSend()
	}()
//	We receive a bunch of messages from the server
// func to receive a bunch of messages from the server
	go func() {
		for {
			res, err := stream.Recv()
			if err == io.EOF{
				break
			}
			if err !=nil {
				log.Fatalf("Error while receiving: %v", err)
			}
			fmt.Printf("Recieved: %v\n", res.GetResult())
		}
		close(waitc)

	}()
	<-waitc
}

func doUnaryWithDeadline(c greetpb.GreetServiceClient, timeout time.Duration){
	fmt.Println("Starting a unary deadline rpc")
	req := &greetpb.GreetWithDeadlineRequest{
		Greeting: &greetpb.Greeting{
			FirstName: "Pavan",
			LastName: "Tikkani",
		},
	}
	//ctx := context.WithValue(context.Background(),"key", "test-run")
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res, err := c.GreetWithDeadline(ctx, req)

	if err != nil{
		statusErr, ok := status.FromError(err)
		if ok {
			if statusErr.Code() == codes.DeadlineExceeded {
				fmt.Println("The timeout has reached! Deadline Exceeded")
			}else{
				fmt.Printf("Unexpected error: %v\n", statusErr)
			}
		}else{
			log.Fatalf("error while calling greet rpc: %v\n", err)
		}
		return

	}
	log.Printf("Response from GreetWithDeadline: %v", res.Result)
}