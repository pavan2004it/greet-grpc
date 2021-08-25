package main

import (
	"context"
	"fmt"
	"go-grpc/greet/greetpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"io"
	"log"
	"net"
	"strconv"
	"time"
)

type server struct {
	greetpb.UnimplementedGreetServiceServer
}

func (*server) Greet(ctx context.Context, req *greetpb.GreetRequest) (*greetpb.GreetResponse, error){
	fmt.Printf("Greet function is invoked with %v\n", req)
	firstName := req.GetGreeting().FirstName
	result := "Hello " + firstName
	res := &greetpb.GreetResponse{
		Result: result,
	}
	return res, nil
}

func (*server) GreetManyTimes(req *greetpb.GreetManyTimesRequest, stream greetpb.GreetService_GreetManyTimesServer) error {
	fmt.Printf("Greet Many Times invoked with %v\n", req)
	firstName := req.GetGreeting().GetFirstName()
	for i:=0; i<10; i++ {
		result := "Hello " + firstName + " number " + strconv.Itoa(i)
		res := &greetpb.GreetManyTimesResponse{
			Result: result,
		}
		stream.SendMsg(res)
		time.Sleep(1000*time.Millisecond)
	}

	fmt.Println("All the messages have been streamed")
	return nil

}

func (*server) LongGreet(stream greetpb.GreetService_LongGreetServer) error{
	fmt.Printf("Long Greet function was invoked with a streaming client: %v\n", stream)
	result := ""
	for{
		req,err := stream.Recv()
		if err == io.EOF{
		//	We have finished reading the stream
			return stream.SendAndClose(&greetpb.LongGreetResponse{
				Result: result,
			})
		}
		if err != nil {
			log.Fatalf("Error while reading streaming client: %v", err)
		}
		firstname := req.GetGreeting().GetFirstName()
		result += "Hello " + firstname + "! "
	}
}

func(*server) GreetEveryone(stream greetpb.GreetService_GreetEveryoneServer) error{
	fmt.Printf("Greet everyone function was invoked with a streaming request: %v", stream)
	for {
		req, err := stream.Recv()
		if err == io.EOF{
			return nil
		}
		if err != nil {
			log.Fatalf("Unable to process the client stream: %v", err)
			return err
		}
		firstName := req.GetGreeting().GetFirstName()
		result := "Hello " + firstName + "! "
		err = stream.Send(&greetpb.GreetEveryoneResponse{
			Result: result,
		})
		if err != nil {
			log.Fatalf("Error while sending data to client: %v", err)
			return err
		}
	}
}

func (*server) GreetWithDeadline(ctx context.Context, req *greetpb.GreetWithDeadlineRequest)(*greetpb.GreetWithDeadlineResponse, error){
	fmt.Printf("Greet function is invoked with %v\n", req)
	for i :=0; i<3 ; i++{
		if ctx.Err() == context.Canceled{
		//	Client cancelled the request
			fmt.Println("Client Cancelled the request")
			return nil, status.Errorf(codes.Canceled, "the client cancelled the request")
		}
		time.Sleep(1*time.Second)
	}
	firstName := req.GetGreeting().FirstName
	result := "Hello " + firstName
	res := &greetpb.GreetWithDeadlineResponse{
		Result: result,
	}
	return res, nil
}

func main(){
	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	certFile := "ssl/server.crt"
	keyFile := "ssl/server.pem"
	creds, sslErr := credentials.NewServerTLSFromFile(certFile, keyFile)
	if sslErr != nil{
		log.Fatalf("Failed to authenticate cert: %v", sslErr)
	}
	opts := grpc.Creds(creds)
	s := grpc.NewServer(opts)
	greetpb.RegisterGreetServiceServer(s, &server{})
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
