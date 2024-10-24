package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
)

type Server struct {
	listener  net.Listener
	directory string
	signals   chan os.Signal
	done      chan bool
}

type Route struct {
	method  string
	pattern string
	handler func(inputs []string)
}

var (
	listen    = flag.String("address", ":4221", "The address to listen on")
	directory = flag.String("directory", "/tmp", "The directory to serve files from")
)

func main() {
	flag.Parse()

	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Println("Failed to bind to port 4221")
		os.Exit(1)
	}

	server := Server{
		listener:  listener,
		directory: *directory,
		signals:   make(chan os.Signal, 1),
		done:      make(chan bool, 1),
	}

	signal.Notify(server.signals, syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM)

	go server.handle_signals()
	go server.accept_connection()

	log.Println("Running... Press Ctrl+C to exit.")
	<-server.done
	log.Println("Exiting program.")
}

func (server *Server) handle_signals() {
	<-server.signals
	log.Println("Received interrupt, shutting down...")
	server.listener.Close()
	server.done <- true
}

func (server *Server) accept_connection() {
	for {
		conn, err := server.listener.Accept()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && !opErr.Temporary() {
				log.Println("Server shutdown in progress...")
				return
			}
			log.Println("Error accepting connection: ", err.Error())
			continue
		}

		go server.handle_connection(conn)
	}
}

func (server *Server) handle_connection(conn net.Conn) {
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
			method:  "GET",
			pattern: "^/$",
			handler: func(inputs []string) {
				conn.Write([]byte("HTTP/1.1 200 OK\r\n\r\n"))
			},
		},
		{
			method:  "GET",
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
			method:  "GET",
			pattern: "^/echo/(.*)$",
			handler: func(inputs []string) {
				content := inputs[1]

				accept_encoding, _ := request.try_get_header("Accept-Encoding")

				var response []byte
				response = append(response, "HTTP/1.1 200 OK\r\n"...)
				response = append(response, "Content-Type: text/plain\r\n"...)
				if strings.Contains(accept_encoding, "gzip") {
					response = append(response, "Content-Encoding: gzip\r\n"...)
				}
				response = append(response, fmt.Sprintf("Content-Length: %d\r\n\r\n", len(content))...)
				response = append(response, content...)

				conn.Write(response)
			},
		},
		{
			method:  "GET",
			pattern: "^/files/(.*)$",
			handler: func(inputs []string) {
				filepath := inputs[1]

				file, err := os.Open(server.directory + "/" + filepath)
				if err != nil {
					conn.Write([]byte("HTTP/1.1 404 Not Found\r\n\r\n"))
					return
				}

				file_info, err := file.Stat()
				if err != nil {
					conn.Write([]byte("HTTP/1.1 500 Internal Server Error\r\n\r\n"))
					return
				}
				file_content := make([]byte, file_info.Size())
				_, err = file.Read(file_content)
				if err != nil {
					conn.Write([]byte("HTTP/1.1 500 Internal Server Error\r\n\r\n"))
					return
				}

				conn.Write([]byte(fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: %d\r\n\r\n%s", file_info.Size(), string(file_content))))
			},
		},
		{
			method:  "POST",
			pattern: "^/files/(.*)$",
			handler: func(inputs []string) {
				filename := inputs[1]

				file, err := os.Create(server.directory + "/" + filename)
				if err != nil {
					conn.Write([]byte("HTTP/1.1 500 Internal Server Error\r\n\r\n"))
					return
				}

				log.Println(len(request.body))
				n, err := file.Write([]byte(request.body))
				if err != nil {
					conn.Write([]byte("HTTP/1.1 500 Internal Server Error\r\n\r\n"))
					return
				}

				log.Printf("Written %d bytes to %s", n, filename)

				conn.Write([]byte("HTTP/1.1 201 Created\r\n\r\n"))
			},
		},
	}

	for _, route := range mux {
		re := regexp.MustCompile(route.pattern)
		inputs := re.FindStringSubmatch(request.path)
		if re.MatchString(request.path) && request.method == route.method {
			route.handler(inputs)
			return
		}
	}

	// Default case
	log.Println("No route matched, returning 404")
	conn.Write([]byte("HTTP/1.1 404 Not Found\r\n\r\n"))
}

func extract_path(request string) string {
	return strings.Split(request, " ")[1]
}
