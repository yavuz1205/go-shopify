package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/r0busta/go-shopify-graphql-model/v4/graph/model"
	"github.com/yavuz1205/go-shopify"
)

func listMetafields(client *shopify.Client) {
	// Get products
	metafields, err := client.Metafield.ListMetafieldDefinitions(context.Background(), model.MetafieldOwnerTypeProduct)
	if err != nil {
		panic(err)
	}

	// Print out the result
	for _, m := range metafields {
		json, _ := json.MarshalIndent(m, "", "  ")
		fmt.Println(string(json))
	}
}
