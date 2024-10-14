package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
)

type Route struct {
	pattern string
	handler func(inputs []string)
}

func main() {
	listener, err := net.Listen("tcp", "0.0.0.0:4221")
	if err != nil {
		log.Println("Failed to bind to port 4221")
		os.Exit(1)
	}

	signals := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(signals, syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM)

	go handle_signals(signals, done, listener)
	go accept_connection(listener)

	log.Println("Running... Press Ctrl+C to exit.")
	<-done
	log.Println("Exiting program.")
}

func handle_signals(signals chan os.Signal, done chan bool, listener net.Listener) {
	<-signals
	log.Println("Received interrupt, shutting down...")
	listener.Close()
	done <- true
}

func accept_connection(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && !opErr.Temporary() {
				log.Println("Server shutdown in progress...")
				return
			}
			log.Println("Error accepting connection: ", err.Error())
			continue
		}

		go handle_connection(conn)
	}
}

func handle_connection(conn net.Conn) {
	defer conn.Close()

	buffer := make([]byte, 4086)
	_, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("Error reading request: ", err.Error())
		os.Exit(1)
	}

	request, err := parse_request(buffer)
	if err != nil {
		fmt.Println("Error parsing request: ", err.Error())
		os.Exit(1)
	}

	mux := []Route{
		{
			pattern: "^/$",
			handler: func(inputs []string) {
				conn.Write([]byte("HTTP/1.1 200 OK\r\n\r\n"))
			},
		},
		{
			pattern: "^/user-agent$",
			handler: func(inputs []string) {
				user_agent, err := request.try_get_header("User-Agent")
				if err != nil {
					conn.Write([]byte("HTTP/1.1 400 Bad Request\r\n\r\n"))
					return
				}
				conn.Write([]byte(fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: %d\r\n\r\n%s", len(user_agent), user_agent)))
			},
		},
		{
			pattern: "^/echo/(.*)$",
			handler: func(inputs []string) {
				content := inputs[1]
				conn.Write([]byte(fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: %d\r\n\r\n%s", len(content), content)))
			},
		},
	}

	for _, route := range mux {
		re := regexp.MustCompile(route.pattern)
		if inputs := re.FindStringSubmatch(request.path); re.MatchString(request.path) {
			route.handler(inputs)
			return
		}
	}

	// Default case
	conn.Write([]byte("HTTP/1.1 404 Not Found\r\n\r\n"))
}

func extract_path(request string) string {
	return strings.Split(request, " ")[1]
}
