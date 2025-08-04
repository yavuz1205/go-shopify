package shopify

import (
	"context"
	"fmt"
	"strings"

	"github.com/r0busta/go-shopify-graphql-model/v4/graph/model"
	log "github.com/sirupsen/logrus"
)

//go:generate mockgen -destination=./mock/metafield_service.go -package=mock . MetafieldService
type MetafieldService interface {
	ListAllShopMetafields(ctx context.Context) ([]model.Metafield, error)
	ListShopMetafieldsByNamespace(ctx context.Context, namespace string) ([]model.Metafield, error)
	ListMetafieldDefinitions(ctx context.Context, ownerType model.MetafieldOwnerType) ([]model.MetafieldDefinition, error)

	GetShopMetafieldByKey(ctx context.Context, namespace, key string) (*model.Metafield, error)

	Delete(ctx context.Context, metafield model.MetafieldIdentifierInput) error
	DeleteBulk(ctx context.Context, metafield []model.MetafieldIdentifierInput) error
}

type MetafieldServiceOp struct {
	client *Client
}

var _ MetafieldService = &MetafieldServiceOp{}

type mutationMetafieldDelete struct {
	MetafieldDeleteResult struct {
		UserErrors []model.UserError `json:"userErrors,omitempty"`
	} `graphql:"metafieldDelete(input: $input)" json:"metafieldDelete"`
}

func (s *MetafieldServiceOp) ListAllShopMetafields(ctx context.Context) ([]model.Metafield, error) {
	q := `
		{
			shop{
				metafields{
					edges{
						node{
							createdAt
							description
							id
							key
							name
							namespace
							ownerType
							updatedAt
							value
							type
						}
					}
				}	  
			}
		}
`

	res := []model.Metafield{}
	err := s.client.BulkOperation.BulkQuery(ctx, q, &res)
	if err != nil {
		return nil, fmt.Errorf("bulk query: %w", err)
	}

	return res, nil
}

func (s *MetafieldServiceOp) ListShopMetafieldsByNamespace(ctx context.Context, namespace string) ([]model.Metafield, error) {
	q := `
		{
			shop{
				metafields(namespace: "$namespace"){
					edges{
						node{
							createdAt
							description
							id
							key
							name
							namespace
							ownerType
							updatedAt
							value
							type
						}
					}
				}	  
			}
		}
`
	q = strings.ReplaceAll(q, "$namespace", namespace)

	res := []model.Metafield{}
	err := s.client.BulkOperation.BulkQuery(ctx, q, &res)
	if err != nil {
		return nil, fmt.Errorf("bulk query: %w", err)
	}

	return res, nil
}

func (s *MetafieldServiceOp) ListMetafieldDefinitions(ctx context.Context, ownerType model.MetafieldOwnerType) ([]model.MetafieldDefinition, error) {
	q := `
		{
			metafieldDefinitions(ownerType: $ownerType) {
				edges {
					cursor
					node {
						id
						name
						key
						namespace
						ownerType
					}
				}
				pageInfo {
					hasNextPage
				}
			}
		}
`
	q = strings.ReplaceAll(q, "$ownerType", string(ownerType))

	res := []model.MetafieldDefinition{}
	err := s.client.BulkOperation.BulkQuery(ctx, q, &res)
	if err != nil {
		return nil, fmt.Errorf("bulk query: %w", err)
	}

	return res, nil
}

func (s *MetafieldServiceOp) GetShopMetafieldByKey(ctx context.Context, namespace, key string) (*model.Metafield, error) {
	var q struct {
		Shop struct {
			Metafield model.Metafield `graphql:"metafield(namespace: $namespace, key: $key)"`
		} `graphql:"shop"`
	}
	vars := map[string]interface{}{
		"namespace": namespace,
		"key":       key,
	}

	err := s.client.gql.Query(ctx, &q, vars)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	return &q.Shop.Metafield, nil
}

func (s *MetafieldServiceOp) DeleteBulk(ctx context.Context, metafields []model.MetafieldIdentifierInput) error {
	for _, m := range metafields {
		err := s.Delete(ctx, m)
		if err != nil {
			log.Warnf("Couldn't delete metafield (%v): %s", m, err)
		}
	}

	return nil
}

func (s *MetafieldServiceOp) Delete(ctx context.Context, metafield model.MetafieldIdentifierInput) error {
	m := mutationMetafieldDelete{}

	vars := map[string]interface{}{
		"input": metafield,
	}
	err := s.client.gql.Mutate(ctx, &m, vars)
	if err != nil {
		return fmt.Errorf("mutation: %w", err)
	}

	if len(m.MetafieldDeleteResult.UserErrors) > 0 {
		return fmt.Errorf("%+v", m.MetafieldDeleteResult.UserErrors)
	}

	return nil
}
