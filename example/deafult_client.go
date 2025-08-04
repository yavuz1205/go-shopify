package main

import (
	"log"
	"os"

	"github.com/joho/godotenv"
	shopify "github.com/yavuz1205/go-shopify"
	graphqlclient "github.com/yavuz1205/go-shopify/graphql"
)

func defaultClient() *shopify.Client {
	err := godotenv.Load("../.env")
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	if os.Getenv("STORE_API_KEY") == "" || os.Getenv("STORE_PASSWORD") == "" || os.Getenv("STORE_NAME") == "" {
		panic("Shopify Admin API Key and/or Password (aka access token) and/or store name not set")
	}

	if os.Getenv("STORE_API_VERSION") != "" {
		apiKey := os.Getenv("STORE_API_KEY")
		accessToken := os.Getenv("STORE_PASSWORD")
		storeName := os.Getenv("STORE_NAME")
		opts := []graphqlclient.Option{
			graphqlclient.WithVersion(os.Getenv("STORE_API_VERSION")),
			graphqlclient.WithPrivateAppAuth(apiKey, accessToken),
		}

		gql := graphqlclient.NewClient(storeName, opts...)

		return shopify.NewClient(shopify.WithGraphQLClient(gql))
	}

	return shopify.NewDefaultClient()
}
