package main

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"sync"
	"time"
)

// VectorClock for versioning
type VectorClock struct {
	clocks map[int]int
}

func NewVectorClock() *VectorClock {
	return &VectorClock{clocks: make(map[int]int)}
}

func (vc *VectorClock) Increment(nodeID int) {
	vc.clocks[nodeID]++
}

func (vc *VectorClock) Merge(other *VectorClock) {
	for node, counter := range other.clocks {
		if counter > vc.clocks[node] {
			vc.clocks[node] = counter
		}
	}
}

func (vc *VectorClock) Copy() *VectorClock {
	newVC := NewVectorClock()
	for node, counter := range vc.clocks {
		newVC.clocks[node] = counter
	}
	return newVC
}

// Node in the Dynamo system
type Node struct {
	id   int
	data map[string]*VersionedValue
	mu   sync.RWMutex
}

// VersionedValue to store values with vector clocks
type VersionedValue struct {
	value string
	clock *VectorClock
}

// Dynamo system
type Dynamo struct {
	nodes []*Node
	ring  []int
	mu    sync.RWMutex
	N     int // Replication factor
	R     int // Read quorum
	W     int // Write quorum
}

// NewDynamo initializes a new Dynamo system
func NewDynamo(nodeCount, replicationFactor, readQuorum, writeQuorum int) *Dynamo {
	nodes := make([]*Node, nodeCount)
	ring := make([]int, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodes[i] = &Node{id: i, data: make(map[string]*VersionedValue)}
		ring[i] = i
	}
	return &Dynamo{nodes: nodes, ring: ring, N: replicationFactor, R: readQuorum, W: writeQuorum}
}

func (d *Dynamo) hash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % len(d.nodes)
}

// Get retrieves the value for a given key
func (d *Dynamo) Get(key string) (string, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	nodeIdx := d.hash(key)
	node := d.nodes[nodeIdx]

	node.mu.RLock()
	defer node.mu.RUnlock()

	if version, exists := node.data[key]; exists {
		return version.value, true
	}

	// Search replicas for the value
	for i := 1; i < d.N; i++ {
		replicaIdx := (nodeIdx + i) % len(d.nodes)
		replica := d.nodes[replicaIdx]

		replica.mu.RLock()
		if version, exists := replica.data[key]; exists {
			replica.mu.RUnlock()
			return version.value, true
		}
		replica.mu.RUnlock()
	}

	return "", false
}

// Put stores a key-value pair
func (d *Dynamo) Put(key string, value string, nodeID int) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	nodeIdx := d.hash(key)
	node := d.nodes[nodeIdx]

	node.mu.Lock()
	defer node.mu.Unlock()

	newClock := NewVectorClock()
	newClock.Increment(nodeID)

	if version, exists := node.data[key]; exists {
		newClock.Merge(version.clock)
	}
	node.data[key] = &VersionedValue{value: value, clock: newClock}

	// Replicate to other nodes
	for i := 1; i < d.N; i++ {
		replicaIdx := (nodeIdx + i) % len(d.nodes)
		replica := d.nodes[replicaIdx]

		replica.mu.Lock()
		replica.data[key] = &VersionedValue{value: value, clock: newClock.Copy()}
		replica.mu.Unlock()
	}
}

// RandomFailure simulates a node failure
func (d *Dynamo) RandomFailure() {
	rand.Seed(time.Now().UnixNano())
	idx := rand.Intn(len(d.nodes))
	fmt.Printf("Node %d failed\n", idx)
	d.nodes[idx] = &Node{id: idx, data: make(map[string]*VersionedValue)}
}

// Compare vector clocks to handle conflicts
func compareVectorClocks(vc1, vc2 *VectorClock) int {
	isGreater, isLess := false, false
	for node, counter1 := range vc1.clocks {
		counter2 := vc2.clocks[node]
		if counter1 > counter2 {
			isGreater = true
		} else if counter1 < counter2 {
			isLess = true
		}
	}
	for node, counter2 := range vc2.clocks {
		if _, exists := vc1.clocks[node]; !exists {
			if counter2 > 0 {
				isLess = true
			}
		}
	}
	if isGreater && isLess {
		return 0 // Concurrent versions
	} else if isGreater {
		return 1
	} else if isLess {
		return -1
	} else {
		return 0 // Identical versions
	}
}

func main() {
	dynamo := NewDynamo(5, 3, 2, 2)

	// Initial Put operation
	dynamo.Put("key1", "value1", 0)
	if val, ok := dynamo.Get("key1"); ok {
		fmt.Println("Got:", val)
	} else {
		fmt.Println("Key not found")
	}

	// Simulate node failure
	dynamo.RandomFailure()

	// Get operation after failure
	if val, ok := dynamo.Get("key1"); ok {
		fmt.Println("Got:", val)
	} else {
		fmt.Println("Key not found")
	}

	// Additional operations to illustrate conflict resolution
	dynamo.Put("key1", "value2", 1)
	dynamo.Put("key1", "value3", 2)

	if val, ok := dynamo.Get("key1"); ok {
		fmt.Println("Got:", val)
	} else {
		fmt.Println("Key not found")
	}
}
