package main

import (
	"fmt"
	"net"
	"os"
	"regexp"
	"strings"
)

type Route struct {
	pattern string
	handler func(inputs []string)
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:4221")
	if err != nil {
		fmt.Println("Failed to bind to port 4221")
		os.Exit(1)
	}

	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	defer conn.Close()

	buffer := make([]byte, 4086)
	_, err = conn.Read(buffer)
	if err != nil {
		fmt.Println("Error reading request: ", err.Error())
		os.Exit(1)
	}

	request, err := parse_request(buffer)
	if err != nil {
		fmt.Println("Error parsing request: ", err.Error())
		os.Exit(1)
	}

	path := extract_path(string(buffer))

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
		if inputs := re.FindStringSubmatch(path); re.MatchString(path) {
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
