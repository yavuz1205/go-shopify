package main

func main() {
	client := defaultClient()
	// client := clientWithToken()
	// client := clientWithVersion()

	// Collections
	// collections(client)

	// Products
	listProducts(client)
	// listMetafields(client)
	// createProduct(client)

	// Bulk operations
	// bulk(client)
}
