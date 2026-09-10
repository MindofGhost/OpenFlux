package main

import (
	"fmt"
	"net/url"
	"strings"
)

// documentURLs implements a repeatable --url flag, keeping first-seen order.
type documentURLs []string

func (u *documentURLs) String() string { return strings.Join(*u, ", ") }

func (u *documentURLs) Set(value string) error {
	value = strings.TrimSpace(value)
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && parsed.Scheme != "http") {
		return fmt.Errorf("document URL must be an absolute HTTP(S) URL")
	}
	for _, existing := range *u {
		if existing == value {
			return nil
		}
	}
	*u = append(*u, value)
	return nil
}

func validateDocumentPool(backend, mode string, urls documentURLs) error {
	if backend == "yandex" && len(urls) == 0 {
		return fmt.Errorf("Yandex transport requires --url")
	}
	if len(urls) > 1 && (backend != "yandex" || mode != "tcp") {
		return fmt.Errorf("multiple --url values require --transport yandex --mode tcp")
	}
	return nil
}
