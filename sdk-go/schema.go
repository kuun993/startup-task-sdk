package startuptask

import "context"

// EnsureSchema creates the coordination tables on the store's database.
func EnsureSchema(ctx context.Context, store Store) error {
	return store.EnsureSchema(ctx)
}
