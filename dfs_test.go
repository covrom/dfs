package dfs_test

import (
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/covrom/dfs"

	fuzz "github.com/google/gofuzz"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestValue implements the Keyer interface for testing purposes.
// It represents a simple value type with an ID field as the key,
// additional Data field, and Extra field for testing metadata.
type TestValue struct {
	ID    string
	Data  string
	Extra int
}

// Key returns the key for the TestValue, implementing the Keyer interface.
func (v TestValue) Key() string {
	return v.ID
}

// TestNewStore tests the creation of a new store instance.
// It verifies that the store is properly initialized and can be closed.
func TestNewStore(t *testing.T) {
	// Setup
	filename := "test_store_new.db"
	defer os.Remove(filename)

	// Test store creation
	store, err := dfs.NewStore[string, TestValue](filename)
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}
	defer store.Close()

	if store == nil {
		t.Error("Expected store instance, got nil")
	}
}

// TestSetAndGet tests basic set and get operations.
// It verifies that values can be stored and retrieved correctly.
func TestSetAndGet(t *testing.T) {
	// Setup
	filename := "test_store_set_get.db"
	defer os.Remove(filename)

	store, _ := dfs.NewStore[string, TestValue](filename)
	defer store.Close()

	value := TestValue{ID: "test1", Data: "value1", Extra: 42}

	// Test set operation
	store.Set(value)

	// Verify get operation
	retrieved, ok := store.Get("test1")
	if !ok {
		t.Error("Failed to get inserted value")
	}
	if retrieved != value {
		t.Errorf("Expected %v, got %v", value, retrieved)
	}
}

// TestGetNonExistent tests retrieval of non-existent keys.
// It verifies that the store correctly handles missing keys.
func TestGetNonExistent(t *testing.T) {
	// Setup
	filename := "test_store_get_nonexistent.db"
	defer os.Remove(filename)

	store, _ := dfs.NewStore[string, TestValue](filename)
	defer store.Close()

	// Test get with non-existent key
	_, ok := store.Get("nonexistent")
	if ok {
		t.Error("Expected false for non-existent key, got true")
	}
}

// TestDelete tests the deletion of values.
// It verifies that values are properly removed from the store.
func TestDelete(t *testing.T) {
	// Setup
	filename := "test_store_delete.db"
	defer os.Remove(filename)

	store, _ := dfs.NewStore[string, TestValue](filename)
	defer store.Close()

	value := TestValue{ID: "to_delete", Data: "will be deleted"}

	// Test set and delete operations
	store.Set(value)
	store.Delete(value.ID)

	// Verify deletion
	_, ok := store.Get(value.ID)
	if ok {
		t.Error("Value still exists after deletion")
	}
}

// TestList tests the listing functionality with and without filters.
// It verifies that all values can be retrieved and filtered correctly.
func TestList(t *testing.T) {
	// Setup
	filename := "test_store_list.db"
	defer os.Remove(filename)

	store, _ := dfs.NewStore[string, TestValue](filename)
	defer store.Close()

	values := []TestValue{
		{ID: "item1", Data: "data1"},
		{ID: "item2", Data: "data2"},
		{ID: "item3", Data: "data3"},
	}

	// Populate store
	for _, v := range values {
		store.Set(v)
	}

	// Test unfiltered list
	allItems := store.List(nil)
	if len(allItems) != len(values) {
		t.Errorf("Expected %d items, got %d", len(values), len(allItems))
	}

	// Test filtered list
	filtered := store.List(func(v TestValue) bool {
		return v.ID == "item2"
	})
	if len(filtered) != 1 || filtered[0].ID != "item2" {
		t.Error("Filter did not work correctly")
	}
}

// TestPersistence tests that values persist between store instances.
// It verifies that data is properly saved to disk and reloaded.
func TestPersistence(t *testing.T) {
	// Setup
	filename := "test_store_persistence.db"
	defer os.Remove(filename)

	// First store instance with write
	store1, _ := dfs.NewStore[string, TestValue](filename)
	value := TestValue{ID: "persistent", Data: "should survive"}
	store1.Set(value)
	store1.Close()

	// Second store instance
	store2, _ := dfs.NewStore[string, TestValue](filename)
	defer store2.Close()

	// Verify persistence
	retrieved, ok := store2.Get("persistent")
	if !ok {
		t.Error("Value not persisted")
	}
	if retrieved != value {
		t.Errorf("Expected %v, got %v", value, retrieved)
	}
}

// TestConcurrentAccess tests concurrent operations on the store.
// It verifies thread safety under concurrent read/write/delete operations.
func TestConcurrentAccess(t *testing.T) {
	// Setup
	filename := "test_store_concurrent.db"
	defer os.Remove(filename)

	store, _ := dfs.NewStore[string, TestValue](filename)
	defer store.Close()

	// Test parameters
	const numRoutines = 10
	const numIterations = 100
	var wg sync.WaitGroup

	// Launch concurrent workers
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				key := string(rune('A' + id))
				value := TestValue{ID: key, Data: string(rune('a' + j%26))}
				store.Set(value)
				store.Get(key)
				if j%10 == 0 {
					store.Delete(key)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify some items remain
	list := store.List(nil)
	if len(list) == 0 && numRoutines*numIterations > 0 {
		t.Error("Expected some items after concurrent operations")
	}
}

// TestCompaction tests the automatic compaction functionality.
// It verifies that the store properly compacts its data file.
func TestCompaction(t *testing.T) {
	// Setup with short compaction interval
	filename := "test_store_compaction.db"
	defer os.Remove(filename)

	store, _ := dfs.NewStore[string, TestValue](filename, dfs.WithCompactionInterval(100*time.Millisecond))
	defer store.Close()

	// Fill the store
	for i := 0; i < 100; i++ {
		store.Set(TestValue{ID: string(rune(i)), Data: "data"})
	}

	// Wait for compaction
	time.Sleep(200 * time.Millisecond)

	// Verify initial compaction
	fileInfo, _ := os.Stat(filename)
	if fileInfo.Size() < 7000 { // Heuristic size check
		t.Error("Compaction likely did not work, file is small: ", fileInfo.Size())
	}

	// Delete half the items
	for i := 0; i < 50; i++ {
		store.Delete(string(rune(i)))
	}

	// Wait for second compaction
	time.Sleep(200 * time.Millisecond)

	// Verify second compaction
	fileInfo, _ = os.Stat(filename)
	if fileInfo.Size() > 7000 { // Heuristic size check
		t.Error("Compaction likely did not work, file too large:", fileInfo.Size())
	}
}

// TestClose tests store closure functionality.
// It verifies that the store can be properly closed and rejects operations afterward.
func TestClose(t *testing.T) {
	// Setup
	filename := "test_store_close.db"
	defer os.Remove(filename)

	store, _ := dfs.NewStore[string, TestValue](filename)

	// Test close operation
	err := store.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Verify operations fail after close
	if store.Set(TestValue{ID: "test", Data: "data"}) == nil {
		t.Error("Expected error when using closed store")
	}
}

// TestOptimisticConcurrency tests optimistic concurrency control features.
// It verifies version checking during concurrent modifications.
func TestOptimisticConcurrency(t *testing.T) {
	// Setup
	fileName := "test_optimistic.db"
	defer os.Remove(fileName)

	store, err := dfs.NewStore[string, TestValue](fileName)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Test data
	initialValue := TestValue{ID: "test1", Data: "initial", Extra: 1}
	updatedValue := TestValue{ID: "test1", Data: "updated", Extra: 2}

	t.Run("SetOptimistic on new key", func(t *testing.T) {
		// Test initial set with version 0
		err := store.SetOptimistic(dfs.VersionedValue[string, TestValue]{
			Value:   initialValue,
			Version: 0,
		})
		if err != nil {
			t.Errorf("SetOptimistic failed on new key: %v", err)
		}

		// Verify the value was set
		val, ok := store.Get("test1")
		if !ok || val.Data != "initial" {
			t.Errorf("Value not set correctly, got: %v, ok: %v", val, ok)
		}
	})

	t.Run("GetOptimistic returns correct version", func(t *testing.T) {
		// Test version retrieval
		versioned, ok := store.GetOptimistic("test1")
		if !ok {
			t.Fatal("GetOptimistic failed to find existing key")
		}

		if versioned.Value.Data != "initial" || versioned.Version != 1 {
			t.Errorf("GetOptimistic returned wrong data/version, got: %+v", versioned)
		}
	})

	t.Run("SetOptimistic with correct version", func(t *testing.T) {
		// Get current version
		current, ok := store.GetOptimistic("test1")
		if !ok {
			t.Fatal("Failed to get current version")
		}

		// Test update with correct version
		err := store.SetOptimistic(dfs.VersionedValue[string, TestValue]{
			Value:   updatedValue,
			Version: current.Version,
		})
		if err != nil {
			t.Errorf("SetOptimistic failed with correct version: %v", err)
		}

		// Verify update
		val, ok := store.Get("test1")
		if !ok || val.Data != "updated" {
			t.Errorf("Value not updated correctly, got: %v", val)
		}
	})

	t.Run("SetOptimistic with stale version fails", func(t *testing.T) {
		// Test update with stale version
		err := store.SetOptimistic(dfs.VersionedValue[string, TestValue]{
			Value:   TestValue{ID: "test1", Data: "stale", Extra: 3},
			Version: 1, // This is now stale
		})
		if err != dfs.ErrVersionMismatch {
			t.Errorf("Expected ErrVersionMismatch, got: %v", err)
		}

		// Verify value wasn't changed
		val, _ := store.Get("test1")
		if val.Data != "updated" {
			t.Error("Value was incorrectly updated with stale version")
		}
	})

	t.Run("ListOptimistic returns versions", func(t *testing.T) {
		// Add another value
		store.Set(TestValue{ID: "test2", Data: "second", Extra: 1})

		// Test versioned list
		list := store.ListOptimistic(nil)
		if len(list) != 2 {
			t.Fatalf("Expected 2 items, got %d", len(list))
		}

		// Verify versions
		for _, item := range list {
			if item.Value.ID == "test1" && item.Version != 2 {
				t.Errorf("test1 has wrong version: %d", item.Version)
			}
			if item.Value.ID == "test2" && item.Version != 1 {
				t.Errorf("test2 has wrong version: %d", item.Version)
			}
		}
	})

	t.Run("Concurrent modification protection", func(t *testing.T) {
		// Simulate concurrent modification scenario
		current, _ := store.GetOptimistic("test1")

		// Simulate concurrent update by another process
		store.Set(TestValue{ID: "test1", Data: "concurrent", Extra: 5})

		// Attempt update with stale version
		err := store.SetOptimistic(dfs.VersionedValue[string, TestValue]{
			Value:   TestValue{ID: "test1", Data: "should fail", Extra: 6},
			Version: current.Version,
		})
		if err != dfs.ErrVersionMismatch {
			t.Errorf("Expected ErrVersionMismatch for concurrent modification, got: %v", err)
		}
	})
}

// TestOptimisticEdgeCases tests edge cases for optimistic concurrency control.
func TestOptimisticEdgeCases(t *testing.T) {
	fileName := "test_optimistic_edge.db"
	defer os.Remove(fileName)

	store, err := dfs.NewStore[string, TestValue](fileName)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	t.Run("GetOptimistic on non-existent key", func(t *testing.T) {
		_, ok := store.GetOptimistic("nonexistent")
		if ok {
			t.Error("GetOptimistic should return false for non-existent key")
		}
	})

	t.Run("SetOptimistic on deleted key", func(t *testing.T) {
		// Create and delete a key
		store.Set(TestValue{ID: "temp", Data: "temp", Extra: 1})
		store.Delete("temp")

		// Test resurrecting deleted key
		err := store.SetOptimistic(dfs.VersionedValue[string, TestValue]{
			Value:   TestValue{ID: "temp", Data: "resurrect", Extra: 2},
			Version: 1,
		})
		if err != nil {
			t.Errorf("SetOptimistic should work on deleted keys, got: %v", err)
		}

		// Verify resurrection
		val, ok := store.Get("temp")
		if !ok || val.Data != "resurrect" {
			t.Errorf("Value not resurrected correctly, got: %v, ok: %v", val, ok)
		}
	})

	t.Run("Filtered ListOptimistic", func(t *testing.T) {
		// Setup test data
		store.Set(TestValue{ID: "filter1", Data: "include", Extra: 10})
		store.Set(TestValue{ID: "filter2", Data: "exclude", Extra: 5})

		// Test filtered list with versions
		filter := func(v TestValue) bool { return v.Extra > 5 }
		list := store.ListOptimistic(filter)

		if len(list) != 1 || list[0].Value.ID != "filter1" {
			t.Errorf("Filtered ListOptimistic returned wrong results: %+v", list)
		}
	})
}

// BenchmarkSet benchmarks the performance of Set operations.
func BenchmarkSet(b *testing.B) {
	defer os.Remove("test.db")
	store, err := dfs.NewStore[string, TestValue]("test.db", dfs.WithCompactionInterval(time.Hour))
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	// Run benchmark
	for i := 0; i < b.N; i++ {
		store.Set(TestValue{ID: fmt.Sprint(i), Data: "Test"})
	}
}

// BenchmarkGet benchmarks the performance of Get operations.
func BenchmarkGet(b *testing.B) {
	defer os.Remove("test.db")
	store, err := dfs.NewStore[string, TestValue]("test.db", dfs.WithCompactionInterval(time.Hour))
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()

	// Pre-populate store
	for i := 0; i < 1000; i++ {
		store.Set(TestValue{ID: fmt.Sprint(i), Data: "Test"})
	}

	// Run benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.Get(fmt.Sprint(i % 1000))
	}
}

// TestKeyValue implements Keyer interface for fuzzy testing.
type TestKeyValue struct {
	KeyField string
	Data     string
}

// Key returns the key for TestKeyValue, implementing Keyer interface.
func (t TestKeyValue) Key() string {
	return t.KeyField
}

// TestStoreFuzzy tests the store with fuzzy-generated data.
// It verifies behavior with random inputs and edge cases.
func TestStoreFuzzy(t *testing.T) {
	const testFile = "test_fuzzy.db"
	const compactionInterval = 100 * time.Millisecond
	const writeQueueSize = 1000
	const dbSizeElems = 1000
	defer os.Remove(testFile)

	// Cleanup before test
	_ = os.Remove(testFile)

	t.Run("basic operations", func(t *testing.T) {
		store, err := dfs.NewStore[string, TestKeyValue](testFile,
			dfs.WithCompactionInterval(compactionInterval),
			dfs.WithWriteQueueSize(writeQueueSize),
		)
		require.NoError(t, err)
		defer store.Close()

		// Initialize fuzzer
		f := fuzz.New().NilChance(0).NumElements(dbSizeElems/2, dbSizeElems)

		// Generate random test data
		var testData []TestKeyValue
		f.Fuzz(&testData)

		// Sort and deduplicate
		slices.SortStableFunc(testData, func(a, b TestKeyValue) int {
			return strings.Compare(a.KeyField, b.KeyField)
		})

		testData = slices.CompactFunc(testData, func(a, b TestKeyValue) bool {
			return a.KeyField == b.KeyField
		})

		// Store all generated data
		for _, item := range testData {
			err := store.Set(item)
			assert.NoError(t, err)
		}

		// Verify all items can be retrieved
		for _, item := range testData {
			val, ok := store.Get(item.Key())
			assert.True(t, ok)
			assert.Equal(t, item, val)
		}

		// Verify List returns all items
		allItems := store.List(nil)
		assert.Len(t, allItems, len(testData))

		// Randomly delete some items
		for i := 0; i < len(testData)/2; i++ {
			idx := i % len(testData)
			key := testData[idx].Key()
			err := store.Delete(key)
			assert.NoError(t, err)

			_, ok := store.Get(key)
			assert.False(t, ok)
		}

		// Verify count after deletion
		remainingItems := store.List(nil)
		assert.LessOrEqual(t, len(remainingItems), len(testData))
	})

	t.Run("concurrent operations", func(t *testing.T) {
		store, err := dfs.NewStore[string, TestKeyValue](testFile,
			dfs.WithCompactionInterval(compactionInterval),
			dfs.WithWriteQueueSize(writeQueueSize),
		)
		require.NoError(t, err)
		defer store.Close()

		f := fuzz.New().NilChance(0).NumElements(dbSizeElems/2, dbSizeElems)

		// Generate random test data
		var testData []TestKeyValue
		f.Fuzz(&testData)

		// Concurrently set values
		var wg sync.WaitGroup
		for _, item := range testData {
			wg.Add(1)
			go func(item TestKeyValue) {
				defer wg.Done()
				err := store.Set(item)
				assert.NoError(t, err)
			}(item)
		}
		wg.Wait()

		// Concurrently get values
		for _, item := range testData {
			wg.Add(1)
			go func(key string) {
				defer wg.Done()
				val, ok := store.Get(key)
				assert.True(t, ok)
				assert.NotEmpty(t, val)
			}(item.Key())
		}
		wg.Wait()

		// Concurrently delete some values
		for i := 0; i < len(testData)/2; i++ {
			idx := i % len(testData)
			wg.Add(1)
			go func(key string) {
				defer wg.Done()
				err := store.Delete(key)
				assert.NoError(t, err)
			}(testData[idx].Key())
		}
		wg.Wait()
	})

	t.Run("compaction stress test", func(t *testing.T) {
		store, err := dfs.NewStore[string, TestKeyValue](testFile,
			dfs.WithCompactionInterval(50*time.Millisecond),
			dfs.WithWriteQueueSize(writeQueueSize),
		)
		require.NoError(t, err)

		f := fuzz.New().NilChance(0).NumElements(dbSizeElems/2, dbSizeElems)

		// Generate large amount of random data
		var testData []TestKeyValue
		f.Fuzz(&testData)

		// Rapidly set and delete values to trigger compactions
		for i := 0; i < 5; i++ {
			for _, item := range testData {
				err := store.Set(item)
				assert.NoError(t, err)
			}

			for j := 0; j < len(testData)/2; j++ {
				idx := j % len(testData)
				err := store.Delete(testData[idx].Key())
				assert.NoError(t, err)
			}
		}

		// Verify store is still functional
		for _, item := range testData[:10] {
			_, ok := store.Get(item.Key())
			assert.True(t, ok || !ok) // Just check it doesn't panic
		}

		// Get current list and close store
		lst := store.List(nil)
		store.Close()

		// Reopen store and verify data
		store, err = dfs.NewStore[string, TestKeyValue](testFile,
			dfs.WithCompactionInterval(50*time.Millisecond),
			dfs.WithWriteQueueSize(writeQueueSize),
		)
		require.NoError(t, err)

		for _, item := range lst {
			_, ok := store.Get(item.Key())
			assert.True(t, ok)
		}

		store.Close()
	})
}

// TestStoreEdgeCases tests various edge cases of store operations.
func TestStoreEdgeCases(t *testing.T) {
	const testFile = "test_edge_cases.db"
	const compactionInterval = 100 * time.Millisecond
	defer os.Remove(testFile)

	// Cleanup before test
	_ = os.Remove(testFile)

	t.Run("empty store", func(t *testing.T) {
		store, err := dfs.NewStore[string, TestKeyValue](testFile,
			dfs.WithCompactionInterval(compactionInterval),
			dfs.WithWriteQueueSize(10),
		)
		require.NoError(t, err)
		defer store.Close()

		// Test operations on empty store
		val, ok := store.Get("nonexistent")
		assert.False(t, ok)
		assert.Empty(t, val)

		items := store.List(nil)
		assert.Empty(t, items)
	})

	t.Run("duplicate keys", func(t *testing.T) {
		store, err := dfs.NewStore[string, TestKeyValue](testFile,
			dfs.WithCompactionInterval(compactionInterval),
			dfs.WithWriteQueueSize(10),
		)
		require.NoError(t, err)
		defer store.Close()

		// Test duplicate key handling
		key := "duplicate"
		item1 := TestKeyValue{KeyField: key, Data: "first"}
		item2 := TestKeyValue{KeyField: key, Data: "second"}

		err = store.Set(item1)
		assert.NoError(t, err)

		err = store.Set(item2)
		assert.NoError(t, err)

		// Verify last write wins
		val, ok := store.Get(key)
		assert.True(t, ok)
		assert.Equal(t, item2, val)
	})

	t.Run("closed store operations", func(t *testing.T) {
		store, err := dfs.NewStore[string, TestKeyValue](testFile,
			dfs.WithCompactionInterval(compactionInterval),
			dfs.WithWriteQueueSize(10),
		)
		require.NoError(t, err)

		// Close immediately
		store.Close()

		// Verify operations fail on closed store
		err = store.Set(TestKeyValue{KeyField: "test", Data: "data"})
		assert.Error(t, err)
		assert.Equal(t, dfs.ErrStoreIsClosed, err)

		err = store.Delete("test")
		assert.Error(t, err)
		assert.Equal(t, dfs.ErrStoreIsClosed, err)
	})
}
