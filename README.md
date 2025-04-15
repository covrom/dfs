# dfs
A high-performance, disk-backed key-value store with versioning and optimistic concurrency control, designed for simplicity and reliability.

## Features

- **Persistent storage**: All data is safely written to disk in an append-only log
- **Human-readable format**: Data stored as JSON lines for easy inspection and debugging
- **Version control**: Every record has a version number that increments on changes
- **Optimistic locking**: Supports concurrent access with version checking
- **Automatic compaction**: Regularly cleans up deleted records and old versions
- **Thread-safe**: Safe for concurrent use by multiple goroutines
- **Simple API**: Easy-to-use interface with minimal boilerplate
- **Configurable**: Tunable performance parameters and logging

## Overview

DFS is a persistent key-value store that combines in-memory caching with disk-based storage. It's designed for applications that need:

- Persistent storage with fast reads
- Version tracking of records
- Optimistic concurrency control
- Human-readable data format for debugging
- Simple integration without complex setup

The store uses an append-only log for writes, providing durability while maintaining good write performance. All data is stored as JSON lines, making it easy to inspect and modify data directly if needed.

Records are automatically versioned, allowing for optimistic concurrency control patterns where clients can detect and handle conflicts. The store periodically compacts its data file to remove old versions and deleted records.

## Implementation Details

### Storage Format

DFS stores data in a simple JSON line format:
```json
{"k":"user123","v":3,"d":false,"val":{"name":"Alice","email":"alice@example.com"}}
{"k":"user456","v":1,"d":true}
```

Each line represents a record with:
- `k`: The record key
- `v`: Version number (increments with each change)
- `d`: Deleted flag
- `val`: The actual value (omitted for deleted records)

### Concurrency Model

DFS uses a single-writer goroutine model:
1. All mutations go through a channel to a dedicated writer goroutine
2. Reads are served directly from the in-memory cache with RW locks
3. The writer batches writes to disk for better performance

### Versioning and Optimistic Locking

Every record maintains a version number that increments with each modification. This enables optimistic concurrency control:
- Clients can read a value and its version
- When updating, they can specify the expected version
- The update fails if the version doesn't match (indicating concurrent modification)

### Compaction

DFS periodically compacts its data file by:
1. Creating a temporary file with only the latest versions of active records
2. Atomically replacing the old file with the new one
3. Continuing to append new changes to the compacted file

This keeps the data file size manageable while maintaining all the benefits of append-only storage.

## Usage

```bash
go get -u github.com/covrom/dfs@latest
go mod tidy
```

### Additional Configuration
For production use, consider these recommended options:

```go
store, err := dfs.NewStore[string, User]("data.db",
    dfs.WithCompactionInterval(5*time.Minute),
    dfs.WithWriteQueueSize(1000),
    dfs.WithLogger(yourLogger),
)
```

## Examples

### Basic Usage

```go
type User struct {
    ID    string `json:"id"`
    Name  string `json:"name"`
    Email string `json:"email"`
}

func (u User) Key() string { return u.ID }

func main() {
    // Create a new store
    store, err := dfs.NewStore[string, User]("users.db")
    if err != nil {
        log.Fatal(err)
    }
    defer store.Close()

    // Store a user
    alice := User{ID: "user123", Name: "Alice", Email: "alice@example.com"}
    if err := store.Set(alice); err != nil {
        log.Fatal(err)
    }

    // Retrieve a user
    if user, ok := store.Get("user123"); ok {
        fmt.Printf("Found user: %v\n", user)
    }

    // List all active users
    allUsers := store.List(nil)
    fmt.Printf("All users: %v\n", allUsers)
}
```

### Optimistic Concurrency Control

```go
func updateEmail(store *dfs.Store[string, User], userID, newEmail string) error {
    // Get current version
    current, ok := store.GetOptimistic(userID)
    if !ok {
        return fmt.Errorf("user not found")
    }

    // Prepare update
    updated := current.Value
    updated.Email = newEmail

    // Try to update with version check
    err := store.SetOptimistic(dfs.VersionedValue[string, User]{
        Value:   updated,
        Version: current.Version,
    })
    
    if errors.Is(err, dfs.ErrVersionMismatch) {
        // Handle concurrent modification
        return fmt.Errorf("user was modified by another process")
    }
    return err
}
```

### Custom Configuration

```go
func main() {
    // Configure store with custom options
    logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
    
    store, err := dfs.NewStore[string, User]("users.db",
        dfs.WithLogger(logger),
        dfs.WithCompactionInterval(5*time.Minute),
        dfs.WithWriteQueueSize(500),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer store.Close()
    
    // ... use store ...
}
```

## Transactions

The `Transaction` type provides atomic operations on the `Store` by batching multiple operations together and committing them as a single unit. Transactions support:

- **Atomicity**: All operations in a transaction succeed or fail together
- **Isolation**: Transactions operate on a snapshot of the data
- **Consistency**: Version checks prevent conflicts with other concurrent operations

### Transaction Lifecycle

1. **Begin**: Start a new transaction with `store.Begin()`
2. **Operations**: Perform `Set` and `Delete` operations
3. **Commit/Rollback**: Either commit changes or rollback the transaction

### Methods

#### `Begin()`

```go
func (s *Store[K, V]) Begin() *Transaction[K, V]
```

Creates and returns a new transaction. The transaction operates on a snapshot of the current store state.

#### `Set(v V) error`

```go
func (t *Transaction[K, V]) Set(v V) error
```

Adds a value to be set in the transaction. The value must implement the `Keyer` interface. Automatically increments the version number.

#### `Delete(key K) error`

```go
func (t *Transaction[K, V]) Delete(key K) error
```

Marks a key for deletion in the transaction. The deletion will only be applied if the key exists and isn't already deleted.

#### `Commit() error`

```go
func (t *Transaction[K, V]) Commit() error
```

Attempts to commit all transaction operations atomically. Returns:

- `nil` on success
- `ErrTransactionConflict` if versions changed since the transaction began
- `ErrStoreIsClosed` if the store was closed during the transaction

#### `Rollback()`

```go
func (t *Transaction[K, V]) Rollback()
```

Cancels the transaction, discarding all pending operations.

### Transaction Example Usage

```go
// Begin transaction
tx := store.Begin()

// Perform operations
err := tx.Set(value1)
if err != nil {
    tx.Rollback()
    return err
}

err = tx.Delete(key2)
if err != nil {
    tx.Rollback()
    return err
}

// Commit changes
if err := tx.Commit(); err != nil {
    // Handle error
    return err
}
```

### Transaction Error Handling

The transaction may return these errors:

- `ErrTransactionConflict`: Version mismatch detected during commit
- `ErrStoreIsClosed`: Store was closed during the transaction

### Transaction Implementation Notes

- Transactions operate on a snapshot of the data (copied at `Begin()`)
- Version numbers are checked at commit time to detect conflicts
- All operations are batched and written asynchronously after successful validation
- Rollback is inexpensive as it just discards pending operations

## Performance Considerations

- **Reads** are extremely fast as they come from memory
- **Writes** are batched and asynchronous for good throughput
- **Compaction** happens in the background without blocking operations
- For best performance, size your write queue appropriately for your workload

## License

DFS is released under the MIT License. See LICENSE file for details.

---

This package is ideal for applications that need a simple, persistent key-value store with version tracking and concurrent access support. The human-readable JSON format makes it particularly suitable for development and debugging scenarios.
