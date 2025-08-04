package shopify

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/r0busta/go-shopify-graphql-model/v4/graph/model"
	log "github.com/sirupsen/logrus"
	"github.com/yavuz1205/go-shopify/rand"
	"github.com/yavuz1205/go-shopify/utils"
	"gopkg.in/guregu/null.v4"
)

const (
	edgesFieldName = "Edges"
	nodeFieldName  = "Node"
)

//go:generate mockgen -destination=./mock/bulk_service.go -package=mock . BulkOperationService
type BulkOperationService interface {
	BulkQuery(ctx context.Context, query string, v interface{}) error

	PostBulkQuery(ctx context.Context, query string) (*string, error)
	GetCurrentBulkQuery(ctx context.Context) (*model.BulkOperation, error)
	GetCurrentBulkQueryResultURL(ctx context.Context) (*string, error)
	WaitForCurrentBulkQuery(ctx context.Context, interval time.Duration) (*model.BulkOperation, error)
	ShouldGetBulkQueryResultURL(ctx context.Context, id *string) (*string, error)
	CancelRunningBulkQuery(ctx context.Context) error
}

type BulkOperationServiceOp struct {
	client *Client
}

var _ BulkOperationService = &BulkOperationServiceOp{}

type mutationBulkOperationRunQuery struct {
	BulkOperationRunQueryResult model.BulkOperationRunQueryPayload `graphql:"bulkOperationRunQuery(query: $query)" json:"bulkOperationRunQuery"`
}

type mutationBulkOperationRunQueryCancel struct {
	BulkOperationCancelResult model.BulkOperationCancelPayload `graphql:"bulkOperationCancel(id: $id)" json:"bulkOperationCancel"`
}

var gidRegex *regexp.Regexp

func init() {
	gidRegex = regexp.MustCompile(`^gid://shopify/(\w+)/\d+`)
}

func (s *BulkOperationServiceOp) PostBulkQuery(ctx context.Context, query string) (*string, error) {
	m := mutationBulkOperationRunQuery{}
	vars := map[string]interface{}{
		"query": null.StringFrom(query),
	}

	err := s.client.gql.Mutate(ctx, &m, vars)
	if err != nil {
		return nil, fmt.Errorf("error posting bulk query: %w", err)
	}
	if len(m.BulkOperationRunQueryResult.UserErrors) > 0 {
		errors, _ := json.MarshalIndent(m.BulkOperationRunQueryResult.UserErrors, "", "    ")
		return nil, fmt.Errorf("error posting bulk query: %s", errors)
	}

	return &m.BulkOperationRunQueryResult.BulkOperation.ID, nil
}

func (s *BulkOperationServiceOp) GetCurrentBulkQuery(ctx context.Context) (*model.BulkOperation, error) {
	var q struct {
		CurrentBulkOperation struct {
			model.BulkOperation
		}
	}
	err := s.client.gql.Query(ctx, &q, nil)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	return &q.CurrentBulkOperation.BulkOperation, nil
}

func (s *BulkOperationServiceOp) GetCurrentBulkQueryResultURL(ctx context.Context) (*string, error) {
	return s.ShouldGetBulkQueryResultURL(ctx, nil)
}

func (s *BulkOperationServiceOp) ShouldGetBulkQueryResultURL(ctx context.Context, id *string) (*string, error) {
	q, err := s.GetCurrentBulkQuery(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting current bulk operation: %w", err)
	}

	if id != nil && q.ID != *id {
		return nil, fmt.Errorf("Bulk operation ID doesn't match, got=%v, want=%v", q.ID, id)
	}

	q, _ = s.WaitForCurrentBulkQuery(ctx, 1*time.Second)
	if q.Status != model.BulkOperationStatusCompleted {
		return nil, fmt.Errorf("Bulk operation didn't complete, status=%s, error_code=%s", q.Status, q.ErrorCode)
	}

	if q.ErrorCode != nil && q.ErrorCode.String() != "" {
		return nil, fmt.Errorf("Bulk operation error: %s", q.ErrorCode)
	}

	if q.ObjectCount == "0" {
		return nil, nil
	}

	if q.URL == nil {
		return nil, fmt.Errorf("empty URL result")
	}

	return q.URL, nil
}

func (s *BulkOperationServiceOp) WaitForCurrentBulkQuery(ctx context.Context, interval time.Duration) (*model.BulkOperation, error) {
	q, err := s.GetCurrentBulkQuery(ctx)
	if err != nil {
		return q, fmt.Errorf("CurrentBulkOperation query error: %w", err)
	}

	for q.Status == model.BulkOperationStatusCreated || q.Status == model.BulkOperationStatusRunning || q.Status == model.BulkOperationStatusCanceling {
		log.Debugf("Bulk operation is still %s...", q.Status)
		time.Sleep(interval)

		q, err = s.GetCurrentBulkQuery(ctx)
		if err != nil {
			return q, fmt.Errorf("CurrentBulkOperation query error: %w", err)
		}
	}
	log.Debugf("Bulk operation ready, latest status=%s", q.Status)

	return q, nil
}

func (s *BulkOperationServiceOp) CancelRunningBulkQuery(ctx context.Context) error {
	q, err := s.GetCurrentBulkQuery(ctx)
	if err != nil {
		return err
	}

	if q.Status == model.BulkOperationStatusCreated || q.Status == model.BulkOperationStatusRunning {
		log.Debugln("Canceling running operation")
		operationID := q.ID

		m := mutationBulkOperationRunQueryCancel{}
		vars := map[string]interface{}{
			"id": operationID,
		}

		err = s.client.gql.Mutate(ctx, &m, vars)
		if err != nil {
			return fmt.Errorf("mutation: %w", err)
		}
		if len(m.BulkOperationCancelResult.UserErrors) > 0 {
			return fmt.Errorf("%+v", m.BulkOperationCancelResult.UserErrors)
		}

		q, err = s.GetCurrentBulkQuery(ctx)
		if err != nil {
			return err
		}
		for q.Status == model.BulkOperationStatusCreated || q.Status == model.BulkOperationStatusRunning || q.Status == model.BulkOperationStatusCanceling {
			log.Tracef("Bulk operation still %s...", q.Status)
			q, err = s.GetCurrentBulkQuery(ctx)
			if err != nil {
				return fmt.Errorf("get current bulk query: %w", err)
			}
		}
		log.Debugln("Bulk operation cancelled")
	}

	return nil
}

func (s *BulkOperationServiceOp) BulkQuery(ctx context.Context, query string, out interface{}) error {
	_, err := s.WaitForCurrentBulkQuery(ctx, 1*time.Second)
	if err != nil {
		return err
	}

	id, err := s.PostBulkQuery(ctx, query)
	if err != nil {
		return fmt.Errorf("post bulk query: %w", err)
	}

	if id == nil {
		return fmt.Errorf("Posted operation ID is nil")
	}

	url, err := s.ShouldGetBulkQueryResultURL(ctx, id)
	if err != nil {
		return fmt.Errorf("get bulk query result URL: %w", err)
	}

	if url == nil || *url == "" {
		return fmt.Errorf("Operation result URL is empty")
	}

	filename := fmt.Sprintf("%s%s", rand.String(10), ".jsonl")
	resultFile := filepath.Join(os.TempDir(), filename)
	defer os.Remove(resultFile) // Avoid storage overflow in high traffic environments
	err = utils.DownloadFile(resultFile, *url)
	if err != nil {
		return fmt.Errorf("download file: %w", err)
	}

	err = parseBulkQueryResult(resultFile, out)
	if err != nil {
		return fmt.Errorf("parse bulk query result: %w", err)
	}

	return nil
}

func parseBulkQueryResult(resultFilePath string, out interface{}) error {
	if reflect.TypeOf(out).Kind() != reflect.Ptr {
		return fmt.Errorf("the out arg is not a pointer")
	}

	outValue := reflect.ValueOf(out)
	outSlice := outValue.Elem()
	if outSlice.Kind() != reflect.Slice {
		return fmt.Errorf("the out arg is not a pointer to a slice interface")
	}

	sliceItemType := outSlice.Type().Elem() // slice item type
	sliceItemKind := sliceItemType.Kind()
	itemType := sliceItemType // slice item underlying type
	if sliceItemKind == reflect.Ptr {
		itemType = itemType.Elem()
	}

	resultPath, err := os.Open(resultFilePath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer utils.CloseFile(resultPath)

	reader := bufio.NewReader(resultPath)
	json := jsoniter.ConfigFastest

	connectionSink := make(map[string]interface{})

	for {
		var line []byte
		line, err = reader.ReadBytes('\n')
		if err != nil {
			break
		}

		parentIDNode := json.Get(line, "__parentId")
		if parentIDNode.LastError() == nil {
			parentID := parentIDNode.ToString()

			gidNode := json.Get(line, "id")
			if gidNode.LastError() != nil {
				gidNode = json.Get(line, "__typename")
				if gidNode.LastError() != nil {
					return fmt.Errorf("The connection type must query the `id` or `__typename` field")
				}
			}
			gid := gidNode.ToString()
			edgeType, nodeType, connectionFieldName, err := concludeObjectType(gid)
			if err != nil {
				return err
			}
			node := reflect.New(nodeType).Interface()
			err = json.Unmarshal(line, &node)
			if err != nil {
				return fmt.Errorf("unmarshalling: %w", err)
			}
			nodeVal := reflect.ValueOf(node).Elem()

			var edge interface{}
			var edgeVal reflect.Value
			var nodeField reflect.Value
			if edgeType.Kind() == reflect.Ptr {
				edge = reflect.New(edgeType.Elem()).Interface()
				nodeField = reflect.ValueOf(edge).Elem().FieldByName(nodeFieldName)
				edgeVal = reflect.ValueOf(edge)
			} else {
				edge = reflect.New(edgeType).Interface()

				if reflect.ValueOf(edge).Kind() == reflect.Ptr {
					nodeField = reflect.ValueOf(edge).Elem().FieldByName(nodeFieldName)
				} else {
					nodeField = reflect.ValueOf(edge).FieldByName(nodeFieldName)
				}

				edgeVal = reflect.ValueOf(edge).Elem()
			}

			if !nodeField.IsValid() {
				return fmt.Errorf("Edge in the '%s' doesn't have the Node field", connectionFieldName)
			}
			nodeField.Set(nodeVal)

			var edgesSlice reflect.Value
			var edges map[string]interface{}
			if val, ok := connectionSink[parentID]; ok {
				var ok2 bool
				if edges, ok2 = val.(map[string]interface{}); !ok2 {
					return fmt.Errorf("The connection sink for parent ID '%s' is not a map", parentID)
				}
			} else {
				edges = make(map[string]interface{})
			}

			if val, ok := edges[connectionFieldName]; ok {
				edgesSlice = reflect.ValueOf(val)
			} else {
				edgesSliceCap := 50
				edgesSlice = reflect.MakeSlice(reflect.SliceOf(edgeType), 0, edgesSliceCap)
			}

			edgesSlice = reflect.Append(edgesSlice, edgeVal)

			edges[connectionFieldName] = edgesSlice.Interface()
			connectionSink[parentID] = edges

			continue
		}

		item := reflect.New(itemType).Interface()
		err = json.Unmarshal(line, &item)
		if err != nil {
			return fmt.Errorf("unmarshalling: %w", err)
		}
		itemVal := reflect.ValueOf(item)

		if sliceItemKind == reflect.Ptr {
			outSlice.Set(reflect.Append(outSlice, itemVal))
		} else {
			outSlice.Set(reflect.Append(outSlice, itemVal.Elem()))
		}
	}

	if len(connectionSink) > 0 {
		err := attachNestedConnections(connectionSink, outSlice)
		if err != nil {
			return fmt.Errorf("error processing nested connections: %w", err)
		}
	}

	// check if ReadBytes returned an error different from EOF
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("reading the result file: %w", err)
	}

	return nil
}

func attachNestedConnections(connectionSink map[string]interface{}, outSlice reflect.Value) error {
	for i := 0; i < outSlice.Len(); i++ {
		parent := outSlice.Index(i)
		if parent.Kind() == reflect.Ptr {
			parent = parent.Elem()
		}

		nodeField := parent.FieldByName("Node")
		if nodeField != (reflect.Value{}) {
			if nodeField.Kind() == reflect.Ptr {
				parent = nodeField.Elem()
			} else if nodeField.Kind() == reflect.Interface {
				parent = nodeField.Elem().Elem()
			} else {
				parent = nodeField
			}
		}

		parentIDField := parent.FieldByName("ID")
		if parentIDField == (reflect.Value{}) {
			return fmt.Errorf("No ID field on the first level")
		}
		if reflect.TypeOf(parentIDField).Kind() == reflect.Ptr {
			parentIDField = parentIDField.Elem()
		}

		var parentID string
		var ok bool
		if parentID, ok = parentIDField.Interface().(string); !ok {
			return fmt.Errorf("ID field on the first level is not a string")
		}

		var connection interface{}
		if connection, ok = connectionSink[parentID]; !ok {
			continue
		}

		edgeMap := reflect.ValueOf(connection)
		iter := edgeMap.MapRange()
		for iter.Next() {
			connectionName := iter.Key()
			connectionField := parent.FieldByName(connectionName.String())
			if !connectionField.IsValid() {
				return fmt.Errorf("Connection '%s' is not defined on the parent type %s", connectionName.String(), parent.Type().String())
			}

			var connectionValue reflect.Value
			var edgesField reflect.Value
			if connectionField.Kind() == reflect.Ptr {
				connectionValue = reflect.ValueOf(reflect.New(connectionField.Type().Elem()).Interface())
				edgesField = connectionValue.Elem().FieldByName(edgesFieldName)
			} else {
				connectionValue = reflect.ValueOf(reflect.New(connectionField.Type()).Interface())
				edgesField = connectionValue.Elem().FieldByName(edgesFieldName)
			}

			if !edgesField.IsValid() {
				return fmt.Errorf("Connection %s in the '%s' doesn't have the Edges field", connectionName.String(), parent.Type().String())
			}

			edges := reflect.ValueOf(iter.Value().Interface())
			edgesField.Set(edges)

			connectionField.Set(connectionValue)

			edgesSlice := iter.Value().Elem()
			if edgesSlice.Len() > 0 {
				firstEdge := edgesSlice.Index(0)
				if firstEdge.Kind() == reflect.Ptr {
					firstEdge = firstEdge.Elem()
				}
				node := firstEdge.FieldByName("Node")
				if node.Kind() == reflect.Ptr {
					node = node.Elem()
				}

				if node.FieldByName("ID").IsValid() {
					err := attachNestedConnections(connectionSink, edgesSlice)
					if err != nil {
						return fmt.Errorf("error attacing a nested connection: %w", err)
					}
				}
			}
		}
	}

	return nil
}

func concludeObjectType(gidOrTypename string) (reflect.Type, reflect.Type, string, error) {
	var resource string
	submatches := gidRegex.FindStringSubmatch(gidOrTypename)
	if len(submatches) == 2 {
		resource = submatches[1]
	} else {
		resource = gidOrTypename
	}

	switch resource {
	case "LineItem":
		return reflect.TypeOf(model.LineItemEdge{}), reflect.TypeOf(&model.LineItem{}), fmt.Sprintf("%ss", resource), nil
	case "FulfillmentOrderLineItem":
		return reflect.TypeOf(model.FulfillmentOrderLineItemEdge{}), reflect.TypeOf(&model.FulfillmentOrderLineItem{}), "LineItems", nil
	case "FulfillmentOrder":
		return reflect.TypeOf(model.FulfillmentOrderEdge{}), reflect.TypeOf(&model.FulfillmentOrder{}), fmt.Sprintf("%ss", resource), nil
	case "MediaImage":
		return reflect.TypeOf(model.MediaEdge{}), reflect.TypeOf(&model.MediaImage{}), "Media", nil
	case "Video":
		return reflect.TypeOf(model.MediaEdge{}), reflect.TypeOf(&model.Video{}), "Media", nil
	case "Model3d":
		return reflect.TypeOf(model.MediaEdge{}), reflect.TypeOf(&model.Model3d{}), "Media", nil
	case "ExternalVideo":
		return reflect.TypeOf(model.MediaEdge{}), reflect.TypeOf(&model.ExternalVideo{}), "Media", nil
	case "Metafield":
		return reflect.TypeOf(model.MetafieldEdge{}), reflect.TypeOf(&model.Metafield{}), fmt.Sprintf("%ss", resource), nil
	case "Order":
		return reflect.TypeOf(model.OrderEdge{}), reflect.TypeOf(&model.Order{}), fmt.Sprintf("%ss", resource), nil
	case "Product":
		return reflect.TypeOf(model.ProductEdge{}), reflect.TypeOf(&model.Product{}), fmt.Sprintf("%ss", resource), nil
	case "ProductVariant":
		return reflect.TypeOf(model.ProductVariantEdge{}), reflect.TypeOf(&model.ProductVariant{}), "Variants", nil
	case "ProductBundleComponent":
		return reflect.TypeOf(model.ProductBundleComponentEdge{}), reflect.TypeOf(&model.ProductBundleComponent{}), "BundleComponents", nil
	case "ProductImage":
		return reflect.TypeOf(model.ImageEdge{}), reflect.TypeOf(&model.Image{}), "Images", nil
	case "Collection":
		return reflect.TypeOf(model.CollectionEdge{}), reflect.TypeOf(&model.Collection{}), "Collections", nil
	case "InventoryLevel":
		return reflect.TypeOf(model.InventoryLevelEdge{}), reflect.TypeOf(&model.InventoryLevel{}), fmt.Sprintf("%ss", resource), nil
	default:
		return reflect.TypeOf(nil), reflect.TypeOf(nil), "", fmt.Errorf("`%s` not implemented type", resource)
	}
}
