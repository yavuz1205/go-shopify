package main

import (
	"os"

	shopify "github.com/yavuz1205/go-shopify"
)

func clientWithToken() *shopify.Client {
	return shopify.NewClientWithToken(os.Getenv("STORE_ACCESS_TOKEN"), os.Getenv("STORE_NAME"))
}
