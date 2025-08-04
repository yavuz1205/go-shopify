package main

import (
	"os"

	shopify "github.com/yavuz1205/go-shopify"
	graphqlclient "github.com/yavuz1205/go-shopify/graphql"
)

func clientWithVersion() *shopify.Client {
	gqlClient := graphqlclient.NewClient(os.Getenv("STORE_NAME"), graphqlclient.WithToken(os.Getenv("STORE_ACCESS_TOKEN")), graphqlclient.WithVersion("2022-10"))

	return shopify.NewClient(shopify.WithGraphQLClient(gqlClient))
}
