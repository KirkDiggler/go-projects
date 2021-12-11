package dynamo

import (
	"context"
	"testing"

	"github.com/KirkDiggler/go-projects/dynamo/inputs/getitem"

	"github.com/KirkDiggler/go-projects/dynamo/inputs/deleteitem"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type testClient struct {
	client Interface
}

func (c *testClient) Delete(ctx context.Context) {
	_, _ = c.client.DeleteItem(ctx, "test", deleteitem.WithKey(map[string]types.AttributeValue{
		"id": &types.AttributeValueMemberS{Value: "bob"},
	}))
}

func (c *testClient) Get(ctx context.Context) {
	_, _ = c.client.GetItem(ctx, "test", getitem.WithKey(map[string]types.AttributeValue{
		"id": &types.AttributeValueMemberS{Value: "bob"},
	}))
}

func TestMockClient_DeleteItem(t *testing.T) {
	t.Run("it accepts variadic functions", func(t *testing.T) {
		fixture := &testClient{client: &Mock{}}
		ctx := context.Background()

		m := fixture.client.(*Mock)
		m.On("DeleteItem",
			ctx, "test",
			deleteitem.NewOptions(
				deleteitem.WithKey(map[string]types.AttributeValue{
					"id": &types.AttributeValueMemberS{Value: "bob"},
				}),
			)).Return(&deleteitem.Result{}, nil)

		fixture.Delete(ctx)

		m.AssertExpectations(t)
	})
}

func TestMockClient_GetItem(t *testing.T) {
	t.Run("it accepts variadic functions", func(t *testing.T) {
		fixture := &testClient{client: &Mock{}}
		ctx := context.Background()

		m := fixture.client.(*Mock)
		m.On("GetItem",
			ctx, "test",
			getitem.NewOptions(
				getitem.WithKey(map[string]types.AttributeValue{
					"id": &types.AttributeValueMemberS{Value: "bob"},
				}),
			)).Return(&getitem.Result{}, nil)

		fixture.Get(ctx)

		m.AssertExpectations(t)
	})
}
