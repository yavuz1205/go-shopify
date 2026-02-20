package shopify

import (
	"context"
	"fmt"
	"maps"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/r0busta/go-shopify-graphql-model/v4/graph/model"
	"golang.org/x/sync/errgroup"
)

var retryAfterPattern = regexp.MustCompile(`retry_after=([0-9]+(?:\.[0-9]+)?)s`)

//go:generate mockgen -destination=./mock/product_service.go -package=mock . ProductService
type ProductService interface {
	List(ctx context.Context, query string) ([]model.Product, error)
	ListAll(ctx context.Context) ([]model.Product, error)
	ListAllExtended(ctx context.Context) ([]model.Product, error)

	Get(ctx context.Context, id string) (*model.Product, error)

	Create(ctx context.Context, product model.ProductCreateInput, media []model.CreateMediaInput) (*string, error)

	Update(ctx context.Context, product model.ProductUpdateInput, media []model.CreateMediaInput) error

	Delete(ctx context.Context, product model.ProductDeleteInput) error

	VariantsBulkCreate(ctx context.Context, id string, input []model.ProductVariantsBulkInput, strategy model.ProductVariantsBulkCreateStrategy) error
	VariantsBulkUpdate(ctx context.Context, id string, input []model.ProductVariantsBulkInput) error
	VariantsBulkReorder(ctx context.Context, id string, input []model.ProductVariantPositionInput) error

	MediaCreate(ctx context.Context, id string, input []model.CreateMediaInput) error

	FetchVariantMetafields(ctx context.Context, variantIDs []string) (map[string][]model.Metafield, error)
}

type ProductServiceOp struct {
	client *Client
}

var _ ProductService = &ProductServiceOp{}

type mutationProductCreate struct {
	ProductCreateResult struct {
		Product *struct {
			ID string `json:"id,omitempty"`
		} `json:"product,omitempty"`

		UserErrors []model.UserError `json:"userErrors,omitempty"`
	} `graphql:"productCreate(product: $product, media: $media)" json:"productCreate"`
}

type mutationProductUpdate struct {
	ProductUpdateResult struct {
		UserErrors []model.UserError `json:"userErrors,omitempty"`
	} `graphql:"productUpdate(product: $product, media: $media)" json:"productUpdate"`
}

type mutationProductDelete struct {
	ProductDeleteResult struct {
		UserErrors []model.UserError `json:"userErrors,omitempty"`
	} `graphql:"productDelete(input: $input)" json:"productDelete"`
}

type mutationProductVariantsBulkCreate struct {
	ProductVariantsBulkCreateResult struct {
		UserErrors []model.UserError `json:"userErrors,omitempty"`
	} `graphql:"productVariantsBulkCreate(productId: $productId, variants: $variants, strategy: $strategy)" json:"productVariantsBulkCreate"`
}

type mutationProductVariantsBulkUpdate struct {
	ProductVariantsBulkUpdateResult struct {
		UserErrors []model.UserError `json:"userErrors,omitempty"`
	} `graphql:"productVariantsBulkUpdate(productId: $productId, variants: $variants)" json:"productVariantsBulkUpdate"`
}

type mutationProductVariantsBulkReorder struct {
	ProductVariantsBulkReorderResult struct {
		UserErrors []model.UserError `json:"userErrors,omitempty"`
	} `graphql:"productVariantsBulkReorder(positions: $positions, productId: $productId)" json:"productVariantsBulkReorder"`
}

type mutationProductCreateMedia struct {
	ProductCreateMediaResult struct {
		MediaUserErrors []model.UserError `json:"mediaUserErrors,omitempty"`
	} `graphql:"productCreateMedia(productId: $productId, media: $media)" json:"productCreateMedia"`
}

const productBaseQuery = `
	id
	handle
	options{
		id
		name
		values
		position
	}
	tags
	title
	description
	descriptionPlainSummary
	priceRangeV2{
		minVariantPrice{
			amount
			currencyCode
		}
		maxVariantPrice{
			amount
			currencyCode
		}
	}
	productType
	vendor
	totalInventory
	onlineStoreUrl
	descriptionHtml
	seo{
		description
		title
	}
	templateSuffix
	customProductType
`

var productQuery = fmt.Sprintf(`
	%s
	bundleComponents {
		edges{
			node{
				quantity
				componentProduct{
					id
				}
              componentVariants {
                edges {
                  node {
                    id
                  }
                }
              }
			}
		}
	}
	variants(first:100, after: $cursor){
		edges{
			node{
				id
				title
				displayName
				sku
				barcode
				selectedOptions{
					name
					value
					optionValue{
						id
						name
					}
				}
				position
				image {
					id
					altText
					height
					width
					url
				}
				compareAtPrice
				price
				inventoryQuantity
				inventoryItem{
					id
					sku
					measurement {
              			weight {
                			unit
               				value
              			}
					}
					unitCost{
						amount
						currencyCode
					}
				}
				availableForSale
				unitPriceMeasurement{
					measuredType
					quantityUnit
					quantityValue
					referenceUnit
					referenceValue
              }
			}
		}
		pageInfo{
			hasNextPage
		}
	}
`, productBaseQuery)

var productBulkQuery = fmt.Sprintf(`
	%s
	variants{
		edges{
			node{
				id
				title
				displayName
				sku
				barcode
				position
				image {
					id
					altText
					height
					width
					url
				}
				compareAtPrice
				price
				inventoryQuantity
				inventoryItem{
					id
					sku
					measurement {
              			weight {
                			unit
               				value
              			}
					}
					unitCost{
						amount
						currencyCode
					}
				}
				selectedOptions{
					name
					value
					optionValue{
						id
						name
					}
				}
				availableForSale
				unitPriceMeasurement{
					measuredType
					quantityUnit
					quantityValue
					referenceUnit
					referenceValue
				}
			}
		}
	}
`, productBaseQuery)

func (s *ProductServiceOp) ListAll(ctx context.Context) ([]model.Product, error) {
	q := fmt.Sprintf(`
		{
			products{
				edges{
					node{
						%s
					}
				}
			}
		}
	`, productBulkQuery)

	res := []model.Product{}
	err := s.client.BulkOperation.BulkQuery(ctx, q, &res)
	if err != nil {
		return []model.Product{}, err
	}

	return res, nil
}

func (s *ProductServiceOp) List(ctx context.Context, query string) ([]model.Product, error) {
	q := fmt.Sprintf(`
		{
			products(query: "$query"){
				edges{
					node{
						%s
					}
				}
			}
		}
	`, productBulkQuery)

	q = strings.ReplaceAll(q, "$query", query)

	baseProducts := []model.Product{}
	err := s.client.BulkOperation.BulkQuery(ctx, q, &baseProducts)
	if err != nil {
		return nil, fmt.Errorf("bulk query: %w", err)
	}

	detailedProducts := make([]model.Product, 0, len(baseProducts))
	for _, p := range baseProducts {
		detailedProduct, err := s.Get(ctx, p.ID)
		if err != nil {
			return nil, fmt.Errorf("getting detailed product for %s: %w", p.ID, err)
		}
		if detailedProduct != nil {
			detailedProducts = append(detailedProducts, *detailedProduct)
		}
	}

	return detailedProducts, nil
}

func (s *ProductServiceOp) Get(ctx context.Context, id string) (*model.Product, error) {
	out, err := s.getPage(ctx, id, "")
	if err != nil {
		return nil, err
	}

	nextPageData := out
	hasNextPage := out.Variants.PageInfo.HasNextPage
	for hasNextPage && len(nextPageData.Variants.Edges) > 0 {
		cursor := nextPageData.Variants.Edges[len(nextPageData.Variants.Edges)-1].Cursor
		nextPageData, err := s.getPage(ctx, id, cursor)
		if err != nil {
			return nil, fmt.Errorf("get page: %w", err)
		}
		out.Variants.Edges = append(out.Variants.Edges, nextPageData.Variants.Edges...)
		hasNextPage = nextPageData.Variants.PageInfo.HasNextPage
	}

	return out, nil
}

func (s *ProductServiceOp) getPage(ctx context.Context, id string, cursor string) (*model.Product, error) {
	q := fmt.Sprintf(`
		query product($id: ID!, $cursor: String) {
			product(id: $id){
				%s
			}
		}
	`, productQuery)

	vars := map[string]interface{}{
		"id": id,
	}
	if cursor != "" {
		vars["cursor"] = cursor
	}

	out := struct {
		Product *model.Product `json:"product"`
	}{}
	err := s.executeGraphQLQueryWithRetry(ctx, q, vars, &out)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	return out.Product, nil
}

func (s *ProductServiceOp) Create(ctx context.Context, product model.ProductCreateInput, media []model.CreateMediaInput) (*string, error) {
	m := mutationProductCreate{}

	vars := map[string]interface{}{
		"product": product,
		"media":   media,
	}

	err := s.client.gql.Mutate(ctx, &m, vars)
	if err != nil {
		return nil, fmt.Errorf("mutation: %w", err)
	}

	if len(m.ProductCreateResult.UserErrors) > 0 {
		return nil, fmt.Errorf("%+v", m.ProductCreateResult.UserErrors)
	}

	return &m.ProductCreateResult.Product.ID, nil
}

func (s *ProductServiceOp) executeGraphQLQueryWithRetry(ctx context.Context, query string, vars map[string]interface{}, out interface{}) error {
	const (
		maxRetries       = 12
		baseDelay        = 500 * time.Millisecond
		maxDelay         = 8 * time.Second
		minThrottleDelay = 1 * time.Second
	)

	var err error
	for i := 0; i < maxRetries; i++ {
		err = s.client.gql.QueryString(ctx, query, vars, out)
		if err == nil {
			return nil
		}

		if !isRetriableThrottleError(err) {
			return err
		}

		delay, hasServerHint := retryDelayFromError(err)
		if hasServerHint {
			delay = min(max(delay, minThrottleDelay), maxDelay)
		} else {
			delay = min(baseDelay*time.Duration(1<<i), maxDelay)
		}
		if delay <= 0 {
			delay = baseDelay
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			continue
		}
	}
	return err
}

func isRetriableThrottleError(err error) bool {
	if err == nil {
		return false
	}

	msg := strings.ToLower(err.Error())

	// Cost-shape errors are not transient and should fail fast.
	if strings.Contains(msg, "max_cost_exceeded") ||
		strings.Contains(msg, "max cost exceeded") ||
		strings.Contains(msg, "single query max cost limit") {
		return false
	}

	return strings.Contains(msg, "throttled") ||
		strings.Contains(msg, "cost limit exceeded") ||
		strings.Contains(msg, "rate limit")
}

func retryDelayFromError(err error) (time.Duration, bool) {
	if err == nil {
		return 0, false
	}

	match := retryAfterPattern.FindStringSubmatch(strings.ToLower(err.Error()))
	if len(match) != 2 {
		return 0, false
	}

	seconds, parseErr := strconv.ParseFloat(match[1], 64)
	if parseErr != nil || seconds <= 0 {
		return 0, false
	}

	return time.Duration(seconds * float64(time.Second)), true
}

func (s *ProductServiceOp) Update(ctx context.Context, product model.ProductUpdateInput, media []model.CreateMediaInput) error {
	m := mutationProductUpdate{}

	vars := map[string]interface{}{
		"product": product,
		"media":   media,
	}
	err := s.client.gql.Mutate(ctx, &m, vars)
	if err != nil {
		return fmt.Errorf("mutation: %w", err)
	}

	if len(m.ProductUpdateResult.UserErrors) > 0 {
		return fmt.Errorf("%+v", m.ProductUpdateResult.UserErrors)
	}

	return nil
}

func (s *ProductServiceOp) Delete(ctx context.Context, product model.ProductDeleteInput) error {
	m := mutationProductDelete{}

	vars := map[string]interface{}{
		"input": product,
	}
	err := s.client.gql.Mutate(ctx, &m, vars)
	if err != nil {
		return fmt.Errorf("mutation: %w", err)
	}

	if len(m.ProductDeleteResult.UserErrors) > 0 {
		return fmt.Errorf("%+v", m.ProductDeleteResult.UserErrors)
	}

	return nil
}

func (s *ProductServiceOp) VariantsBulkCreate(ctx context.Context, id string, input []model.ProductVariantsBulkInput, strategy model.ProductVariantsBulkCreateStrategy) error {
	m := mutationProductVariantsBulkCreate{}

	vars := map[string]interface{}{
		"productId": id,
		"variants":  input,
		"strategy":  strategy,
	}
	err := s.client.gql.Mutate(ctx, &m, vars)
	if err != nil {
		return fmt.Errorf("mutation: %w", err)
	}

	if len(m.ProductVariantsBulkCreateResult.UserErrors) > 0 {
		return fmt.Errorf("%+v", m.ProductVariantsBulkCreateResult.UserErrors)
	}

	return nil
}

func (s *ProductServiceOp) VariantsBulkUpdate(ctx context.Context, id string, input []model.ProductVariantsBulkInput) error {
	m := mutationProductVariantsBulkUpdate{}

	vars := map[string]interface{}{
		"productId": id,
		"variants":  input,
	}
	err := s.client.gql.Mutate(ctx, &m, vars)
	if err != nil {
		return fmt.Errorf("mutation: %w", err)
	}

	if len(m.ProductVariantsBulkUpdateResult.UserErrors) > 0 {
		return fmt.Errorf("%+v", m.ProductVariantsBulkUpdateResult.UserErrors)
	}

	return nil
}

func (s *ProductServiceOp) VariantsBulkReorder(ctx context.Context, id string, input []model.ProductVariantPositionInput) error {
	m := mutationProductVariantsBulkReorder{}

	vars := map[string]interface{}{
		"productId": id,
		"positions": input,
	}
	err := s.client.gql.Mutate(ctx, &m, vars)
	if err != nil {
		return fmt.Errorf("mutation: %w", err)
	}

	if len(m.ProductVariantsBulkReorderResult.UserErrors) > 0 {
		return fmt.Errorf("%+v", m.ProductVariantsBulkReorderResult.UserErrors)
	}

	return nil
}

func (s *ProductServiceOp) MediaCreate(ctx context.Context, id string, input []model.CreateMediaInput) error {
	m := mutationProductCreateMedia{}

	vars := map[string]interface{}{
		"productId": id,
		"media":     input,
	}

	err := s.client.gql.Mutate(ctx, &m, vars)
	if err != nil {
		return fmt.Errorf("mutation: %w", err)
	}

	if len(m.ProductCreateMediaResult.MediaUserErrors) > 0 {
		return fmt.Errorf("%+v", m.ProductCreateMediaResult.MediaUserErrors)
	}

	return nil
}

func (s *ProductServiceOp) FetchVariantMetafields(ctx context.Context, variantIDs []string) (map[string][]model.Metafield, error) {
	if len(variantIDs) == 0 {
		return make(map[string][]model.Metafield), nil
	}

	result := make(map[string][]model.Metafield)
	const (
		batchSize            = 5
		maxConcurrentBatches = 2
	)

	var mu sync.Mutex
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrentBatches)

	for i := 0; i < len(variantIDs); i += batchSize {
		start := i
		end := min(i+batchSize, len(variantIDs))
		batch := append([]string(nil), variantIDs[start:end]...)

		g.Go(func() error {
			batchResult, err := s.fetchVariantMetafieldsBatch(ctx, batch)
			if err != nil {
				return fmt.Errorf("fetch batch %d-%d: %w", start, end, err)
			}

			mu.Lock()
			maps.Copy(result, batchResult)
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return result, nil
}

func (s *ProductServiceOp) fetchVariantMetafieldsBatch(ctx context.Context, variantIDs []string) (map[string][]model.Metafield, error) {
	gqlIDs := make([]string, len(variantIDs))
	for i, id := range variantIDs {
		if !strings.HasPrefix(id, "gid://shopify/ProductVariant/") {
			gqlIDs[i] = fmt.Sprintf("gid://shopify/ProductVariant/%s", id)
		} else {
			gqlIDs[i] = id
		}
	}

	query := `{
		nodes(ids: [` + `"` + strings.Join(gqlIDs, `","`) + `"` + `]) {
			... on ProductVariant {
				id
				metafields(first: 100) {
					edges {
						node {
							id
							definition {
								id
								name
								namespace
								key
							}
							namespace
							key
							value
							type
							references(first: 10) {
								edges {
									cursor
									node {
										__typename
										... on MediaImage {
											id
											image {
												id
												url
											}
										}
										... on Video {
											id
											sources {
												url
												width
												height
												format
												mimeType
											}
										}
										... on GenericFile {
											id
											url
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}`

	out := struct {
		Nodes []struct {
			ID         string `json:"id"`
			Metafields struct {
				Edges []struct {
					Node struct {
						ID         string `json:"id"`
						Definition *struct {
							ID        string `json:"id"`
							Name      string `json:"name"`
							Namespace string `json:"namespace"`
							Key       string `json:"key"`
						} `json:"definition"`
						Namespace  string `json:"namespace"`
						Key        string `json:"key"`
						Value      string `json:"value"`
						Type       string `json:"type"`
						References *struct {
							Edges []struct {
								Cursor string `json:"cursor"`
								Node   struct {
									Typename string `json:"__typename"`
									ID       string `json:"id"`
									URL      string `json:"url"`
									Image    *struct {
										ID  string `json:"id"`
										URL string `json:"url"`
									} `json:"image"`
									Sources []struct {
										URL    string `json:"url"`
										Height int    `json:"height"`
										Format string `json:"format"`
									} `json:"sources"`
								} `json:"node"`
							} `json:"edges"`
						} `json:"references"`
					} `json:"node"`
				} `json:"edges"`
			} `json:"metafields"`
		} `json:"nodes"`
	}{}

	err := s.executeGraphQLQueryWithRetry(ctx, query, nil, &out)
	if err != nil {
		return nil, fmt.Errorf("query variant metafields: %w", err)
	}

	result := make(map[string][]model.Metafield)
	for _, node := range out.Nodes {
		metafields := make([]model.Metafield, 0)
		for _, edge := range node.Metafields.Edges {
			metafield := model.Metafield{
				ID:        edge.Node.ID,
				Namespace: edge.Node.Namespace,
				Key:       edge.Node.Key,
				Value:     edge.Node.Value,
				Type:      edge.Node.Type,
			}

			if edge.Node.Definition != nil {
				metafield.Definition = &model.MetafieldDefinition{
					ID:        edge.Node.Definition.ID,
					Name:      edge.Node.Definition.Name,
					Namespace: edge.Node.Definition.Namespace,
					Key:       edge.Node.Definition.Key,
				}
			}

			if edge.Node.References != nil {
				refEdges := make([]model.MetafieldReferenceEdge, 0)
				for _, refEdge := range edge.Node.References.Edges {
					if refEdge.Node.Typename != "" {
						var node model.MetafieldReference
						if refEdge.Node.Typename == "MediaImage" && refEdge.Node.Image != nil {
							node = &model.MediaImage{
								ID: refEdge.Node.ID,
								Image: &model.Image{
									ID:  model.NewString(refEdge.Node.Image.ID),
									URL: refEdge.Node.Image.URL,
								},
							}
						} else if refEdge.Node.Typename == "GenericFile" {
							node = &model.GenericFile{
								ID:  refEdge.Node.ID,
								URL: &refEdge.Node.URL,
							}
						} else if refEdge.Node.Typename == "Video" {
							sources := make([]model.VideoSource, len(refEdge.Node.Sources))
							for i, s := range refEdge.Node.Sources {
								sources[i] = model.VideoSource{
									URL:    s.URL,
									Height: s.Height,
									Format: s.Format,
								}
							}
							node = &model.Video{
								ID:      refEdge.Node.ID,
								Sources: sources,
							}
						}

						if node != nil {
							refEdges = append(refEdges, model.MetafieldReferenceEdge{
								Cursor: refEdge.Cursor,
								Node:   node,
							})
						}
					}
				}
				metafield.References = &model.MetafieldReferenceConnection{
					Edges: refEdges,
				}
			}

			metafields = append(metafields, metafield)
		}
		result[node.ID] = metafields
	}

	return result, nil
}

func (s *ProductServiceOp) ListAllExtended(ctx context.Context) ([]model.Product, error) {
	products, err := s.ListAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("list products: %w", err)
	}

	var allProductIDs []string
	var allVariantIDs []string
	productMap := make(map[string]*model.Product)
	productVariantMap := make(map[string]*model.Product)
	variantIndexMap := make(map[string]int)

	for i := range products {
		allProductIDs = append(allProductIDs, products[i].ID)
		productMap[products[i].ID] = &products[i]
		for j, edge := range products[i].Variants.Edges {
			if edge.Node != nil {
				allVariantIDs = append(allVariantIDs, edge.Node.ID)
				productVariantMap[edge.Node.ID] = &products[i]
				variantIndexMap[edge.Node.ID] = j
			}
		}
	}

	// Create an errgroup to fetch extended data concurrently
	g, ctx := errgroup.WithContext(ctx)

	// Fetch Inventory Levels
	g.Go(func() error {
		inventoryLevels, err := s.fetchInventoryLevels(ctx, allVariantIDs)
		if err != nil {
			return fmt.Errorf("fetch inventory levels: %w", err)
		}
		for variantID, levels := range inventoryLevels {
			if product, exists := productVariantMap[variantID]; exists {
				variantIndex := variantIndexMap[variantID]
				if variantIndex < len(product.Variants.Edges) && product.Variants.Edges[variantIndex].Node != nil {
					l := levels
					product.Variants.Edges[variantIndex].Node.InventoryItem.InventoryLevels = &l
				}
			}
		}
		return nil
	})

	// Fetch Bundle Components
	g.Go(func() error {
		bundleComponents, err := s.fetchBundleComponents(ctx, allProductIDs)
		if err != nil {
			return fmt.Errorf("fetch bundle components: %w", err)
		}
		for productID, components := range bundleComponents {
			if product, exists := productMap[productID]; exists {
				c := components
				product.BundleComponents = &c
			}
		}
		return nil
	})

	// Fetch Variant Metafields
	g.Go(func() error {
		variantMetafields, err := s.FetchVariantMetafields(ctx, allVariantIDs)
		if err != nil {
			return fmt.Errorf("fetch variant metafields: %w", err)
		}
		for variantID, metafields := range variantMetafields {
			if product, exists := productVariantMap[variantID]; exists {
				variantIndex := variantIndexMap[variantID]
				if variantIndex < len(product.Variants.Edges) && product.Variants.Edges[variantIndex].Node != nil {
					edges := make([]model.MetafieldEdge, len(metafields))
					for i, mf := range metafields {
						edges[i] = model.MetafieldEdge{Node: &mf}
					}
					product.Variants.Edges[variantIndex].Node.Metafields = &model.MetafieldConnection{
						Edges: edges,
					}
				}
			}
		}
		return nil
	})

	// Fetch Resource Publications
	g.Go(func() error {
		resourcePublications, err := s.fetchResourcePublications(ctx, allProductIDs)
		if err != nil {
			return fmt.Errorf("fetch resource publications: %w", err)
		}
		for productID, publications := range resourcePublications {
			if product, exists := productMap[productID]; exists {
				p := publications
				product.ResourcePublications = &p
			}
		}
		return nil
	})

	// Fetch Product Metafields
	g.Go(func() error {
		productMetafields, err := s.fetchProductMetafields(ctx, allProductIDs)
		if err != nil {
			return fmt.Errorf("fetch product metafields: %w", err)
		}
		for productID, metafields := range productMetafields {
			if product, exists := productMap[productID]; exists {
				m := metafields
				product.Metafields = &m
			}
		}
		return nil
	})

	// Fetch Product Collections
	g.Go(func() error {
		productCollections, err := s.fetchProductCollections(ctx, allProductIDs)
		if err != nil {
			return fmt.Errorf("fetch product collections: %w", err)
		}
		for productID, collections := range productCollections {
			if product, exists := productMap[productID]; exists {
				c := collections
				product.Collections = &c
			}
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return products, nil
}

func (s *ProductServiceOp) fetchProductCollections(ctx context.Context, productIDs []string) (map[string]model.CollectionConnection, error) {
	if len(productIDs) == 0 {
		return make(map[string]model.CollectionConnection), nil
	}

	result := make(map[string]model.CollectionConnection)
	const batchSize = 20

	for i := 0; i < len(productIDs); i += batchSize {
		end := min(i+batchSize, len(productIDs))

		batch := productIDs[i:end]
		batchResult, err := s.fetchProductCollectionsBatch(ctx, batch)
		if err != nil {
			return nil, fmt.Errorf("fetch product collections batch %d-%d: %w", i, end, err)
		}

		maps.Copy(result, batchResult)
	}

	return result, nil
}

func (s *ProductServiceOp) fetchProductCollectionsBatch(ctx context.Context, productIDs []string) (map[string]model.CollectionConnection, error) {
	gqlIDs := make([]string, len(productIDs))
	copy(gqlIDs, productIDs)

	const collectionsQuery = `
		collections(first: 100) {
			edges {
				node {
					id
					title
					handle
				}
			}
		}
	`

	query := `{
		nodes(ids: [` + `"` + strings.Join(gqlIDs, `","`) + `"` + `]) {
			... on Product {
				id
				` + collectionsQuery + `
			}
		}
	}`

	out := struct {
		Nodes []struct {
			ID          string                     `json:"id"`
			Collections model.CollectionConnection `json:"collections"`
		} `json:"nodes"`
	}{}

	err := s.executeGraphQLQueryWithRetry(ctx, query, nil, &out)
	if err != nil {
		return nil, fmt.Errorf("query product collections: %w", err)
	}

	result := make(map[string]model.CollectionConnection)
	for _, node := range out.Nodes {
		if node.ID != "" {
			result[node.ID] = node.Collections
		}
	}

	return result, nil
}

func (s *ProductServiceOp) fetchResourcePublications(ctx context.Context, productIDs []string) (map[string]model.ResourcePublicationConnection, error) {
	if len(productIDs) == 0 {
		return make(map[string]model.ResourcePublicationConnection), nil
	}

	result := make(map[string]model.ResourcePublicationConnection)
	const batchSize = 20

	for i := 0; i < len(productIDs); i += batchSize {
		end := min(i+batchSize, len(productIDs))

		batch := productIDs[i:end]
		batchResult, err := s.fetchResourcePublicationsBatch(ctx, batch)
		if err != nil {
			return nil, fmt.Errorf("fetch resource publications batch %d-%d: %w", i, end, err)
		}

		maps.Copy(result, batchResult)
	}

	return result, nil
}

func (s *ProductServiceOp) fetchResourcePublicationsBatch(ctx context.Context, productIDs []string) (map[string]model.ResourcePublicationConnection, error) {
	gqlIDs := make([]string, len(productIDs))
	copy(gqlIDs, productIDs)

	const resourcePublicationsQuery = `
		resourcePublications(first: 30) {
			edges {
				node {
					isPublished
					publication {
						id
						app {
							id
							handle
							title
						}
					}
				}
			}
		}
	`

	query := `{
		nodes(ids: [` + `"` + strings.Join(gqlIDs, `","`) + `"` + `]) {
			... on Product {
				id
				` + resourcePublicationsQuery + `
			}
		}
	}`

	out := struct {
		Nodes []struct {
			ID                   string                              `json:"id"`
			ResourcePublications model.ResourcePublicationConnection `json:"resourcePublications"`
		} `json:"nodes"`
	}{}

	err := s.executeGraphQLQueryWithRetry(ctx, query, nil, &out)
	if err != nil {
		return nil, fmt.Errorf("query resource publications: %w", err)
	}

	result := make(map[string]model.ResourcePublicationConnection)
	for _, node := range out.Nodes {
		if node.ID != "" {
			result[node.ID] = node.ResourcePublications
		}
	}

	return result, nil
}

func (s *ProductServiceOp) fetchProductMetafields(ctx context.Context, productIDs []string) (map[string]model.MetafieldConnection, error) {
	if len(productIDs) == 0 {
		return make(map[string]model.MetafieldConnection), nil
	}

	result := make(map[string]model.MetafieldConnection)
	const batchSize = 20

	for i := 0; i < len(productIDs); i += batchSize {
		end := min(i+batchSize, len(productIDs))

		batch := productIDs[i:end]
		batchResult, err := s.fetchProductMetafieldsBatch(ctx, batch)
		if err != nil {
			return nil, fmt.Errorf("fetch product metafields batch %d-%d: %w", i, end, err)
		}

		maps.Copy(result, batchResult)
	}

	return result, nil
}

func (s *ProductServiceOp) fetchProductMetafieldsBatch(ctx context.Context, productIDs []string) (map[string]model.MetafieldConnection, error) {
	gqlIDs := make([]string, len(productIDs))
	copy(gqlIDs, productIDs)

	const metafieldsQuery = `
		metafields(first: 100) {
			edges {
				node {
					id
					definition {
						id
						name
						namespace
						key
					}
					namespace
					key
					value
					type
					references(first: 10) {
						edges {
							cursor
							node {
								__typename
								... on MediaImage {
									id
									image {
										id
										url
									}
								}
								... on Video {
									id
									sources {
										url
										width
										height
										format
										mimeType
									}
								}
								... on GenericFile {
									id
									url
								}
							}
						}
					}
				}
			}
		}
	`

	query := `{
		nodes(ids: [` + `"` + strings.Join(gqlIDs, `","`) + `"` + `]) {
			... on Product {
				id
				` + metafieldsQuery + `
			}
		}
	}`

	out := struct {
		Nodes []struct {
			ID         string `json:"id"`
			Metafields struct {
				Edges []struct {
					Node struct {
						ID         string `json:"id"`
						Definition *struct {
							ID        string `json:"id"`
							Name      string `json:"name"`
							Namespace string `json:"namespace"`
							Key       string `json:"key"`
						} `json:"definition"`
						Namespace  string `json:"namespace"`
						Key        string `json:"key"`
						Value      string `json:"value"`
						Type       string `json:"type"`
						References *struct {
							Edges []struct {
								Cursor string `json:"cursor"`
								Node   struct {
									Typename string `json:"__typename"`
									ID       string `json:"id"`
									URL      string `json:"url"`
									Image    *struct {
										ID  string `json:"id"`
										URL string `json:"url"`
									} `json:"image"`
									Sources []struct {
										URL      string `json:"url"`
										Height   int    `json:"height"`
										Format   string `json:"format"`
										MimeType string `json:"mimeType"`
									} `json:"sources"`
								} `json:"node"`
							} `json:"edges"`
						} `json:"references"`
					} `json:"node"`
				} `json:"edges"`
			} `json:"metafields"`
		} `json:"nodes"`
	}{}

	err := s.executeGraphQLQueryWithRetry(ctx, query, nil, &out)
	if err != nil {
		return nil, fmt.Errorf("query product metafields: %w", err)
	}

	result := make(map[string]model.MetafieldConnection)
	for _, node := range out.Nodes {
		if node.ID != "" {
			metafieldEdges := make([]model.MetafieldEdge, 0)
			for _, edge := range node.Metafields.Edges {
				metafield := model.Metafield{
					ID:        edge.Node.ID,
					Namespace: edge.Node.Namespace,
					Key:       edge.Node.Key,
					Value:     edge.Node.Value,
					Type:      edge.Node.Type,
				}

				if edge.Node.Definition != nil {
					metafield.Definition = &model.MetafieldDefinition{
						ID:        edge.Node.Definition.ID,
						Name:      edge.Node.Definition.Name,
						Namespace: edge.Node.Definition.Namespace,
						Key:       edge.Node.Definition.Key,
					}
				}

				if edge.Node.References != nil {
					refEdges := make([]model.MetafieldReferenceEdge, 0)
					for _, refEdge := range edge.Node.References.Edges {
						if refEdge.Node.Typename != "" {
							var referenceNode model.MetafieldReference
							if refEdge.Node.Typename == "MediaImage" && refEdge.Node.Image != nil {
								referenceNode = &model.MediaImage{
									ID: refEdge.Node.ID,
									Image: &model.Image{
										ID:  model.NewString(refEdge.Node.Image.ID),
										URL: refEdge.Node.Image.URL,
									},
								}
							} else if refEdge.Node.Typename == "GenericFile" {
								referenceNode = &model.GenericFile{
									ID:  refEdge.Node.ID,
									URL: &refEdge.Node.URL,
								}
							} else if refEdge.Node.Typename == "Video" {
								sources := make([]model.VideoSource, len(refEdge.Node.Sources))
								for i, s := range refEdge.Node.Sources {
									sources[i] = model.VideoSource{
										URL:      s.URL,
										Height:   s.Height,
										Format:   s.Format,
										MimeType: s.MimeType,
									}
								}
								referenceNode = &model.Video{
									ID:      refEdge.Node.ID,
									Sources: sources,
								}
							}

							if referenceNode != nil {
								refEdges = append(refEdges, model.MetafieldReferenceEdge{
									Cursor: refEdge.Cursor,
									Node:   referenceNode,
								})
							}
						}
					}
					metafield.References = &model.MetafieldReferenceConnection{
						Edges: refEdges,
					}
				}
				metafieldEdges = append(metafieldEdges, model.MetafieldEdge{Node: &metafield})
			}
			result[node.ID] = model.MetafieldConnection{
				Edges: metafieldEdges,
			}
		}
	}
	return result, nil
}

func (s *ProductServiceOp) fetchBundleComponents(ctx context.Context, productIDs []string) (map[string]model.ProductBundleComponentConnection, error) {
	if len(productIDs) == 0 {
		return make(map[string]model.ProductBundleComponentConnection), nil
	}

	result := make(map[string]model.ProductBundleComponentConnection)
	const batchSize = 10

	for i := 0; i < len(productIDs); i += batchSize {
		end := min(i+batchSize, len(productIDs))

		batch := productIDs[i:end]
		batchResult, err := s.fetchBundleComponentsBatch(ctx, batch)
		if err != nil {
			return nil, fmt.Errorf("fetch bundle components batch %d-%d: %w", i, end, err)
		}

		maps.Copy(result, batchResult)
	}

	return result, nil
}

func (s *ProductServiceOp) fetchBundleComponentsBatch(ctx context.Context, productIDs []string) (map[string]model.ProductBundleComponentConnection, error) {
	gqlIDs := make([]string, len(productIDs))
	copy(gqlIDs, productIDs)

	const bundleComponentsQuery = `
		bundleComponents(first: 100) {
		  edges {
			node {
			  quantity
			  componentVariants(first: 50) {
				edges {
				  node {
					id
				  }
				}
			  }
			}
		  }
		}
	`

	query := `{
		nodes(ids: [` + `"` + strings.Join(gqlIDs, `","`) + `"` + `]) {
			... on Product {
				id
				` + bundleComponentsQuery + `
			}
		}
	}`

	out := struct {
		Nodes []struct {
			ID               string                                 `json:"id"`
			BundleComponents model.ProductBundleComponentConnection `json:"bundleComponents"`
		} `json:"nodes"`
	}{}

	err := s.executeGraphQLQueryWithRetry(ctx, query, nil, &out)
	if err != nil {
		return nil, fmt.Errorf("query bundle components: %w", err)
	}

	result := make(map[string]model.ProductBundleComponentConnection)
	for _, node := range out.Nodes {
		if node.ID != "" {
			result[node.ID] = node.BundleComponents
		}
	}

	return result, nil
}

func (s *ProductServiceOp) fetchInventoryLevels(ctx context.Context, variantIDs []string) (map[string]model.InventoryLevelConnection, error) {
	if len(variantIDs) == 0 {
		return make(map[string]model.InventoryLevelConnection), nil
	}

	result := make(map[string]model.InventoryLevelConnection)
	const (
		batchSize            = 10
		maxConcurrentBatches = 2
	)

	var mu sync.Mutex
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrentBatches)

	for i := 0; i < len(variantIDs); i += batchSize {
		start := i
		end := min(i+batchSize, len(variantIDs))
		batch := append([]string(nil), variantIDs[start:end]...)

		g.Go(func() error {
			batchResult, err := s.fetchInventoryLevelsBatch(ctx, batch)
			if err != nil {
				return fmt.Errorf("fetch inventory levels batch %d-%d: %w", start, end, err)
			}

			mu.Lock()
			maps.Copy(result, batchResult)
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return result, nil
}

func (s *ProductServiceOp) fetchInventoryLevelsBatch(ctx context.Context, variantIDs []string) (map[string]model.InventoryLevelConnection, error) {
	gqlIDs := make([]string, len(variantIDs))
	copy(gqlIDs, variantIDs)

	const inventoryLevelsQuery = `
			inventoryLevels(first: 10) {
				edges {
					node {
						id
						location {
							id
							name
							shipsInventory
						}
						quantities(names: ["available"]) {
							id
							name
							quantity
					}
				}
			}
		}
	`

	query := `{
		nodes(ids: [` + `"` + strings.Join(gqlIDs, `","`) + `"` + `]) {
			... on ProductVariant {
				id
				inventoryItem {
					id
					` + inventoryLevelsQuery + `
				}
			}
		}
	}`

	out := struct {
		Nodes []struct {
			ID            string `json:"id"`
			InventoryItem *struct {
				InventoryLevels model.InventoryLevelConnection `json:"inventoryLevels"`
			} `json:"inventoryItem"`
		} `json:"nodes"`
	}{}

	err := s.executeGraphQLQueryWithRetry(ctx, query, nil, &out)
	if err != nil {
		return nil, fmt.Errorf("query inventory levels: %w", err)
	}

	result := make(map[string]model.InventoryLevelConnection)
	for _, node := range out.Nodes {
		if node.ID != "" && node.InventoryItem != nil {
			result[node.ID] = node.InventoryItem.InventoryLevels
		}
	}

	return result, nil
}
