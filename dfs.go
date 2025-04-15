package dfs

import (
	"bufio"
	"errors"
	"log/slog"
	"os"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
)

var (
	// ErrStoreIsClosed is returned when attempting to perform operations on a closed store
	ErrStoreIsClosed = errors.New("store is closed")

	// ErrVersionMismatch is returned when an optimistic update fails due to version conflict
	ErrVersionMismatch = errors.New("version mismatch")
)

// Keyer defines an interface for types that can provide their own key
type Keyer[K comparable] interface {
	// Key returns the key associated with the value
	Key() K
}

// StoreOption defines a functional option type for configuring the Store
type StoreOption func(*storeOpts)

// WithCompactionInterval sets the interval at which automatic compaction will occur
func WithCompactionInterval(interval time.Duration) StoreOption {
	return func(s *storeOpts) {
		s.compactionInterval = interval
	}
}

// WithWriteQueueSize sets the size of the asynchronous write queue
func WithWriteQueueSize(size int) StoreOption {
	return func(s *storeOpts) {
		s.writeQueueSize = size
	}
}

// WithLogger sets a custom logger for the Store
func WithLogger(logger *slog.Logger) StoreOption {
	return func(s *storeOpts) {
		s.logger = logger
	}
}

// cacheEntry represents an entry in the in-memory cache
type cacheEntry[V any] struct {
	version int  // version number of the entry
	deleted bool // whether the entry is marked as deleted
	value   V    // the actual stored value
}

// fileRecord represents the format of records stored in the data file
type fileRecord[K comparable, V Keyer[K]] struct {
	Key     K    `json:"k"`   // the key of the record
	Version int  `json:"v"`   // version number of the record
	Deleted bool `json:"d"`   // whether the record is marked as deleted
	Value   V    `json:"val"` // the stored value
}

// command represents an operation to be performed by the writer goroutine
type command[K comparable, V Keyer[K]] struct {
	record  *fileRecord[K, V] // record to be written (nil for compaction-only commands)
	compact bool              // whether compaction should be considered after this operation
}

// storeOpts contains configuration options for the Store
type storeOpts struct {
	logger             *slog.Logger  // logger for store operations
	compactionInterval time.Duration // interval between automatic compactions
	writeQueueSize     int           // size of the write queue
}

// Store is a persistent key-value storage with versioning and compaction support
type Store[K comparable, V Keyer[K]] struct {
	cache      map[K]cacheEntry[V] // in-memory cache of records
	mu         sync.RWMutex        // mutex for concurrent access
	file       *os.File            // underlying data file
	writerChan chan command[K, V]  // channel for writer commands
	done       chan struct{}       // channel for shutdown signal
	wg         sync.WaitGroup      // wait group for goroutines
	filePath   string              // path to the data file
	json       jsoniter.API        // JSON encoder/decoder
	storeOpts                      // embedded configuration options
	closed     bool                // whether the store is closed
}

// NewStore creates a new Store instance with the given filename and options
// fileName: path to the data file (will be created if it doesn't exist)
// opts: optional configuration parameters
// Returns a new Store instance or an error if initialization fails
func NewStore[K comparable, V Keyer[K]](fileName string, opts ...StoreOption) (*Store[K, V], error) {
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	json := jsoniter.ConfigCompatibleWithStandardLibrary
	wantCompact := false

	// Initialize cache by reading existing records from file
	cache := make(map[K]cacheEntry[V])
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var rec fileRecord[K, V]
		if err := json.Unmarshal([]byte(scanner.Text()), &rec); err != nil {
			continue
		}

		// skip lower versions if we've already loaded higher ones
		exists, ok := cache[rec.Key]
		wantCompact = wantCompact || ok

		if ok && rec.Version < exists.version {
			continue
		}

		if rec.Deleted {
			delete(cache, rec.Key)
			wantCompact = true
		} else {
			cache[rec.Key] = cacheEntry[V]{
				version: rec.Version,
				deleted: false,
				value:   rec.Value,
			}
		}
	}

	if err := scanner.Err(); err != nil {
		file.Close()
		return nil, err
	}

	s := &Store[K, V]{
		cache:    cache,
		file:     file,
		done:     make(chan struct{}),
		filePath: fileName,
		json:     json,
		storeOpts: storeOpts{
			logger:             slog.Default(),
			compactionInterval: time.Minute, // default 1 minute
			writeQueueSize:     100,         // default 100
		},
	}

	// Apply configuration options
	for _, opt := range opts {
		opt(&s.storeOpts)
	}

	s.writerChan = make(chan command[K, V], s.writeQueueSize)

	s.wg.Add(2)

	go s.writer(wantCompact)

	if s.compactionInterval > 0 {
		go s.startTicker(s.compactionInterval)
	} else {
		s.wg.Done() // no compaction ticker will be started
	}

	return s, nil
}

// writer is the main goroutine that handles asynchronous writes and compaction
// wantCompact: indicates whether compaction should be performed when possible
func (s *Store[K, V]) writer(wantCompact bool) {
	defer s.wg.Done()

	for {
		select {
		case <-s.done:
			// Drain remaining commands before shutting down
			for len(s.writerChan) > 0 {
				cmd := <-s.writerChan
				s.execCmd(cmd, &wantCompact)
			}
			return
		case cmd := <-s.writerChan:
			s.execCmd(cmd, &wantCompact)
		}
	}
}

// execCmd executes a single writer command
// cmd: the command to execute
// wantCompact: pointer to flag indicating whether compaction is needed
func (s *Store[K, V]) execCmd(cmd command[K, V], wantCompact *bool) {
	if cmd.record != nil {
		data, err := s.json.Marshal(cmd.record)
		if err != nil {
			return
		}
		if _, err := s.file.Write(data); err != nil {
			return
		}
		if _, err := s.file.WriteString("\n"); err != nil {
			return
		}
		if cmd.compact {
			*wantCompact = true
		}
	}
	if cmd.compact {
		if *wantCompact {
			s.logger.Debug("database compaction", "file", s.filePath)
			s.compact()
			*wantCompact = false
		}
	}
}

// compact performs file compaction by rewriting the file with only live records
func (s *Store[K, V]) compact() {
	s.logger.Debug("compaction started")
	s.mu.RLock()
	// Create a snapshot of live records
	snapshot := make(map[K]cacheEntry[V])
	for k, entry := range s.cache {
		if !entry.deleted {
			snapshot[k] = entry
		}
	}
	s.mu.RUnlock()

	tmpPath := s.filePath + ".tmp"
	tempFile, err := os.Create(tmpPath)
	if err != nil {
		s.logger.Error("failed to create temporary compact database", "file", tmpPath, "err", err)
		return
	}

	s.logger.Debug("temporary compact database created")

	writer := bufio.NewWriter(tempFile)
	for k, entry := range snapshot {
		rec := fileRecord[K, V]{
			Version: entry.version,
			Key:     k,
			Deleted: false,
			Value:   entry.value,
		}
		data, err := s.json.Marshal(rec)
		if err != nil {
			s.logger.Error("failed to marshal record", "err", err)
			continue
		}
		if _, err := writer.Write(data); err != nil {
			s.logger.Error("failed to write marshaled record", "err", err)
			continue
		}
		if _, err := writer.WriteString("\n"); err != nil {
			s.logger.Error("failed to write line delimiter", "err", err)
			continue
		}
	}
	if err := writer.Flush(); err != nil {
		s.logger.Error("failed to flush temporary compact database", "err", err)
		return
	}
	if err := tempFile.Close(); err != nil {
		s.logger.Error("failed to close temporary compact database", "err", err)
		return
	}

	s.logger.Debug("compaction preparation completed")

	if err := s.file.Close(); err != nil {
		s.logger.Error("failed to close database", "err", err)
		return
	}

	if err := os.Rename(tmpPath, s.filePath); err != nil {
		s.logger.Error("failed to rename database", "err", err)
		return
	}

	s.file, err = os.OpenFile(s.filePath, os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		s.logger.Error("failed to open database", "err", err)
		return
	}

	s.logger.Debug("compaction completed")
}

// startTicker starts a periodic ticker that triggers compaction
// interval: duration between compaction checks
func (s *Store[K, V]) startTicker(interval time.Duration) {
	defer s.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.writerChan <- command[K, V]{compact: true}
		case <-s.done:
			return
		}
	}
}

// Get retrieves a value by its key
// key: the key to look up
// Returns the value and true if found, or zero value and false if not found
func (s *Store[K, V]) Get(key K) (V, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.cache[key]
	if !ok || entry.deleted {
		var zero V
		return zero, false
	}
	return entry.value, true
}

// Set stores a new value or updates an existing one
// v: the value to store (must implement Keyer interface)
// Returns an error if the store is closed
func (s *Store[K, V]) Set(v V) error {
	key := v.Key()

	s.mu.Lock()

	if s.closed {
		s.mu.Unlock()
		return ErrStoreIsClosed
	}

	newVersion := 1
	wantCompact := false
	if entry, ok := s.cache[key]; ok {
		newVersion = entry.version + 1
		wantCompact = true
	}

	c := command[K, V]{
		record: &fileRecord[K, V]{
			Version: newVersion,
			Key:     key,
			Deleted: false,
			Value:   v,
		},
		compact: wantCompact,
	}

	ce := cacheEntry[V]{
		version: newVersion,
		deleted: false,
		value:   v,
	}

	s.cache[key] = ce

	s.mu.Unlock()

	select {
	case <-s.done:
		return ErrStoreIsClosed
	case s.writerChan <- c:
	}

	return nil
}

// Delete marks a record as deleted
// key: the key of the record to delete
// Returns an error if the store is closed
func (s *Store[K, V]) Delete(key K) error {
	s.mu.Lock()

	if s.closed {
		s.mu.Unlock()
		return ErrStoreIsClosed
	}

	newVersion := 1
	wantCompact := false
	if entry, ok := s.cache[key]; ok {
		if !entry.deleted {
			newVersion = entry.version + 1
			wantCompact = true
		}
	}

	c := command[K, V]{
		record: &fileRecord[K, V]{
			Version: newVersion,
			Key:     key,
			Deleted: true,
		},
		compact: wantCompact,
	}

	ce := cacheEntry[V]{
		version: newVersion,
		deleted: true,
	}

	s.cache[key] = ce

	s.mu.Unlock()

	select {
	case <-s.done:
		return ErrStoreIsClosed
	case s.writerChan <- c:
	}

	return nil
}

// List returns all values that satisfy the given filter
// filter: optional function to filter results (nil returns all values)
// Returns a slice of matching values
func (s *Store[K, V]) List(filter func(V) bool) []V {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []V
	for _, entry := range s.cache {
		if !entry.deleted && (filter == nil || filter(entry.value)) {
			result = append(result, entry.value)
		}
	}
	return result
}

// VersionedValue represents a value with its version number
type VersionedValue[K comparable, V Keyer[K]] struct {
	Value   V   // the stored value
	Version int // current version of the value
}

// GetOptimistic retrieves a value with its version number
// key: the key to look up
// Returns the VersionedValue and true if found, or zero value and false if not found
func (s *Store[K, V]) GetOptimistic(key K) (VersionedValue[K, V], bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.cache[key]
	if !ok || entry.deleted {
		var zero VersionedValue[K, V]
		return zero, false
	}
	return VersionedValue[K, V]{
		Value:   entry.value,
		Version: entry.version,
	}, true
}

// SetOptimistic updates a value only if the provided version matches the current version
// v: the VersionedValue to update
// Returns ErrVersionMismatch if versions don't match, or ErrStoreIsClosed if store is closed
func (s *Store[K, V]) SetOptimistic(v VersionedValue[K, V]) error {
	key := v.Value.Key()

	s.mu.Lock()

	if s.closed {
		s.mu.Unlock()
		return ErrStoreIsClosed
	}

	currentEntry, exists := s.cache[key]
	if exists && !currentEntry.deleted && currentEntry.version != v.Version {
		s.mu.Unlock()
		return ErrVersionMismatch
	}

	newVersion := v.Version + 1
	wantCompact := exists // Only compact if we're updating an existing record

	c := command[K, V]{
		record: &fileRecord[K, V]{
			Version: newVersion,
			Key:     key,
			Deleted: false,
			Value:   v.Value,
		},
		compact: wantCompact,
	}

	ce := cacheEntry[V]{
		version: newVersion,
		deleted: false,
		value:   v.Value,
	}

	s.cache[key] = ce

	s.mu.Unlock()

	select {
	case <-s.done:
		return ErrStoreIsClosed
	case s.writerChan <- c:
	}

	return nil
}

// ListOptimistic returns all values with their versions that satisfy the given filter
// filter: optional function to filter results (nil returns all values)
// Returns a slice of matching VersionedValue instances
func (s *Store[K, V]) ListOptimistic(filter func(V) bool) []VersionedValue[K, V] {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []VersionedValue[K, V]
	for _, entry := range s.cache {
		if !entry.deleted && (filter == nil || filter(entry.value)) {
			result = append(result, VersionedValue[K, V]{
				Value:   entry.value,
				Version: entry.version,
			})
		}
	}
	return result
}

// Close shuts down the store gracefully, waiting for all operations to complete
// Returns an error if closing the underlying file fails
func (s *Store[K, V]) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()

	close(s.done)

	s.wg.Wait()

	close(s.writerChan)
	s.file.Sync()
	return s.file.Close()
}
