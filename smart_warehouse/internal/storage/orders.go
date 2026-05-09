package storage

import (
	"context"
	"fmt"

	"smart_warehouse/internal/events"
)

func (s *CassandraStore) getOrderItems(ctx context.Context, orderID string) ([]events.OrderItem, error) {
	iter := s.session.Query(`
		SELECT product_sku,
		       zone_id,
		       quantity
		FROM order_items_by_order
		WHERE order_id = ?
	`, orderID).WithContext(ctx).Iter()

	var items []events.OrderItem
	var item events.OrderItem
	for iter.Scan(&item.ProductSKU, &item.ZoneID, &item.Quantity) {
		items = append(items, item)
		item = events.OrderItem{}
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("select order items order_id=%s: %w", orderID, err)
	}

	return items, nil
}
