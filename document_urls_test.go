package main

import (
	"flag"
	"reflect"
	"testing"
)

func TestRepeatedDocumentURLs(t *testing.T) {
	var urls documentURLs
	f := flag.NewFlagSet("test", flag.ContinueOnError)
	f.Var(&urls, "url", "document")
	if err := f.Parse([]string{"--url", "https://disk.yandex.ru/i/one", "--url", " https://disk.yandex.ru/i/two ", "--url", "https://disk.yandex.ru/i/one"}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(urls, documentURLs{"https://disk.yandex.ru/i/one", "https://disk.yandex.ru/i/two"}) {
		t.Fatalf("URLs: %v", urls)
	}
	if err := validateDocumentPool("yandex", "tcp", urls); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		backend, mode string
		urls          documentURLs
	}{
		{"yandex", "socks5", urls}, {"oneme", "tcp", urls}, {"yandex", "tcp", nil},
	} {
		if validateDocumentPool(tc.backend, tc.mode, tc.urls) == nil {
			t.Errorf("accepted invalid pool: %+v", tc)
		}
	}
	for _, value := range []string{"", "not-a-url", "file:///tmp/doc", "https://"} {
		if err := urls.Set(value); err == nil {
			t.Errorf("accepted invalid URL %q", value)
		}
	}
	if err := validateDocumentPool("yandex", "socks5", urls[:1]); err != nil {
		t.Fatal(err)
	}
	if err := validateDocumentPool("oneme", "tcp", nil); err != nil {
		t.Fatal(err)
	}
}
