# DFS (Disk-backed File Store)

A high-performance, persistent key-value store with versioning, transactions, and optimistic concurrency control, designed for simplicity and reliability.

## Features

- **Persistent storage**: All data is safely written to disk in an append-only log
- **Human-readable format**: Data stored as JSON lines for easy inspection and debugging
- **Version control**: Every record has a version number that increments on changes
- **ACID Transactions**: Atomic operations with version checking and conflict detection
- **Optimistic locking**: Supports concurrent access with version checking
- **Automatic compaction**: Regularly cleans up deleted records and old versions
- **Thread-safe**: Safe for concurrent use by multiple goroutines
- **Simple API**: Easy-to-use interface with minimal boilerplate
- **Configurable**: Tunable performance parameters and logging

## Overview

DFS is a persistent key-value store that combines in-memory caching with disk-based storage. It's designed for applications that need:

- Persistent storage with fast reads
- Version tracking of records
- Transaction support for atomic operations
- Optimistic concurrency control
- Human-readable data format for debugging
- Simple integration without complex setup

The store uses an append-only log for writes, providing durability while maintaining good write performance. All data is stored as JSON lines, making it easy to inspect and modify data directly if needed.

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

Every record maintains a version number that increments with each modification. This enables:
- Optimistic concurrency control
- Transaction conflict detection
- Change tracking

### Transaction Support

DFS provides ACID transactions with:
- **Atomicity**: All operations in a transaction succeed or fail together
- **Consistency**: Version checks prevent conflicts
- **Isolation**: Transactions operate on a snapshot of the data
- **Durability**: Committed changes are persisted to disk

### Compaction

DFS periodically compacts its data file by:
1. Creating a temporary file with only the latest versions of active records
2. Atomically replacing the old file with the new one
3. Continuing to append new changes to the compacted file

## Installation

```bash
go get -u github.com/covrom/dfs@latest
go mod tidy
```

## Basic Usage

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

## Transaction Usage

```go
// Begin transaction
tx := store.Begin()

// Perform operations
err := tx.Set(User{ID: "user1", Name: "Alice"})
if err != nil {
    tx.Rollback()
    return err
}

err = tx.Delete("user2")
if err != nil {
    tx.Rollback()
    return err
}

// Commit changes
if err := tx.Commit(); err != nil {
    if errors.Is(err, dfs.ErrTransactionConflict) {
        fmt.Println("Conflict detected - retry transaction")
    }
    return err
}
```

## Optimistic Concurrency Control

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

## Configuration Options

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

### Available Options

- `WithCompactionInterval(duration)`: Set automatic compaction interval
- `WithWriteQueueSize(size)`: Configure write queue buffer size
- `WithLogger(logger)`: Set custom logger

## Error Handling

DFS may return these errors:

- `ErrStoreIsClosed`: Operations attempted on closed store
- `ErrVersionMismatch`: Optimistic update version conflict
- `ErrTransactionConflict`: Transaction commit conflict

## Performance Considerations

- **Reads**: Extremely fast from memory cache
- **Writes**: Asynchronous with batching for throughput
- **Transactions**: Snapshot-based with version validation
- **Compaction**: Background process with minimal impact

For best performance:
- Size write queue appropriately for your workload
- Adjust compaction interval based on update frequency
- Use transactions for batch operations

## Testing

The package includes extensive tests covering:
- Basic CRUD operations
- Transaction behavior
- Concurrency scenarios
- Edge cases
- Fuzzy testing with random inputs

Run tests with:
```bash
go test -v ./...
```

## License

DFS is released under the MIT License. See LICENSE file for details.

---

This package is ideal for applications that need:
- Simple persistent key-value storage
- Transaction support
- Version tracking
- Concurrent access
- Human-readable data format

The combination of transactions and optimistic concurrency control makes it particularly suitable for applications requiring data consistency in concurrent environments.