package main

import (
	"fmt"
	"strings"
)

type Request struct {
	method       string
	path         string
	http_version string
	headers      []Header
}

type Header struct {
	key   string
	value string
}

// Parses a header from a string. Returns an error if the header is invalid.
func try_parse_header(line string) (Header, error) {
	parts := strings.Split(line, ": ")

	if len(parts) != 2 {
		return Header{}, fmt.Errorf("Invalid header: %s", line)
	}
	return Header{key: parts[0], value: parts[1]}, nil
}

func parse_request(request_ []byte) (Request, error) {
	request := string(request_)
	lines := strings.Split(request, "\r\n")
	if len(lines) < 1 {
		return Request{}, fmt.Errorf("Invalid request")
	}

	request_line := strings.Split(lines[0], " ")
	if len(request_line) != 3 {
		return Request{}, fmt.Errorf("Invalid request line: %s", lines[0])
	}

	headers := []Header{}
	for _, line := range lines[1:] {
		if line == "" {
			break
		}
		header, err := try_parse_header(line)
		if err != nil {
			return Request{}, err
		}
		headers = append(headers, header)
	}

	return Request{
		method:       request_line[0],
		path:         request_line[1],
		http_version: request_line[2],
		headers:      headers,
	}, nil
}

func (request *Request) try_get_header(key string) (string, error) {
	for _, header := range request.headers {
		if header.key == key {
			return header.value, nil
		}
	}

	return "", fmt.Errorf("Header not found: %s", key)
}
