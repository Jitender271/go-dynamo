package main

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"hash"
	"math/big"
	"sync"
	"time"
)

// hashString calculates the SHA-1 hash of a given string and returns it as a hexadecimal string
func hashString(s string) string {
	h := sha1.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

// Data structure for storing key-value pairs
type Data struct {
	Value       string
	VectorClock map[string]int
}

// Merkle Tree Node
type MerkleTreeNode struct {
	Hash  string
	Left  *MerkleTreeNode
	Right *MerkleTreeNode
}

// Merkle Tree structure
type MerkleTree struct {
	Root  *MerkleTreeNode
	Data  map[string]string
	mutex sync.Mutex
}

// New Merkle Tree
func NewMerkleTree() *MerkleTree {
	return &MerkleTree{
		Data: make(map[string]string),
	}
}

// Insert data into the Merkle Tree
func (mt *MerkleTree) Insert(key, value string) {
	mt.mutex.Lock()
	defer mt.mutex.Unlock()
	mt.Data[key] = value
	mt.build()
}

// Build the Merkle Tree
func (mt *MerkleTree) build() {
	var nodes []*MerkleTreeNode
	for _, value := range mt.Data {
		nodes = append(nodes, &MerkleTreeNode{Hash: hashString(value)})
	}
	for len(nodes) > 1 {
		var newLevel []*MerkleTreeNode
		for i := 0; i < len(nodes); i += 2 {
			if i+1 == len(nodes) {
				newLevel = append(newLevel, nodes[i])
			} else {
				combinedHash := hashString(nodes[i].Hash + nodes[i+1].Hash)
				newNode := &MerkleTreeNode{
					Hash:  combinedHash,
					Left:  nodes[i],
					Right: nodes[i+1],
				}
				newLevel = append(newLevel, newNode)
			}
		}
		nodes = newLevel
	}
	if len(nodes) == 1 {
		mt.Root = nodes[0]
	}
}

// Compare two Merkle Trees
func (mt *MerkleTree) Compare(other *MerkleTree) (bool, *MerkleTreeNode, *MerkleTreeNode) {
	return compareNodes(mt.Root, other.Root)
}

// Compare Merkle Tree Nodes
func compareNodes(a, b *MerkleTreeNode) (bool, *MerkleTreeNode, *MerkleTreeNode) {
	if a == nil && b == nil {
		return true, nil, nil
	}
	if a == nil || b == nil || a.Hash != b.Hash {
		return false, a, b
	}
	leftEqual, leftA, leftB := compareNodes(a.Left, b.Left)
	if !leftEqual {
		return false, leftA, leftB
	}
	rightEqual, rightA, rightB := compareNodes(a.Right, b.Right)
	if !rightEqual {
		return false, rightA, rightB
	}
	return true, nil, nil
}

// Node structure representing a node in the Dynamo ring
type Node struct {
	ID        string
	Data      map[string]Data
	Available bool
	Merkle    *MerkleTree
	mutex     sync.Mutex
}

// NewNode creates a new node
func NewNode(id string) *Node {
	return &Node{
		ID:        id,
		Data:      make(map[string]Data),
		Available: true,
		Merkle:    NewMerkleTree(),
	}
}

// Put inserts or updates a key-value pair in the node
func (n *Node) Put(key, value string, vectorClock map[string]int) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.Data[key] = Data{Value: value, VectorClock: vectorClock}
	n.Merkle.Insert(key, value)
}

// Get retrieves a key-value pair from the node
func (n *Node) Get(key string) (Data, bool) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	data, exists := n.Data[key]
	return data, exists
}

// Gossip simulates gossip protocol
func (n *Node) Gossip(nodes map[string]*Node) {
	if !n.Available {
		return
	}
	for _, node := range nodes {
		if node.ID != n.ID && node.Available {
			n.mutex.Lock()
			for key, data := range n.Data {
				node.Put(key, data.Value, data.VectorClock)
			}
			n.mutex.Unlock()
			fmt.Printf("Node %s gossiped state information to Node %s\n", n.ID, node.ID)
		}
	}
}

// CheckFailures simulates failure detection
func (n *Node) CheckFailures(nodes map[string]*Node) {
	if !n.Available {
		fmt.Printf("Node %s is failed\n", n.ID)
	} else {
		fmt.Printf("Node %s is passed\n", n.ID)
	}
}

// Dynamo structure representing the Dynamo system
type Dynamo struct {
	Nodes     map[string]*Node
	N         int // Replication factor
	R         int // Read quorum
	W         int // Write quorum
	hashFunc  hash.Hash
	hashMutex sync.Mutex
}

// NewDynamo creates a new Dynamo system
func NewDynamo(n, r, w int) *Dynamo {
	return &Dynamo{
		Nodes:    make(map[string]*Node),
		N:        n,
		R:        r,
		W:        w,
		hashFunc: sha1.New(),
	}
}

// AddNode adds a new node to the Dynamo system
func (d *Dynamo) AddNode(id string) {
	d.Nodes[id] = NewNode(id)
}

// GetHash calculates the hash of a given key
func (d *Dynamo) GetHash(key string) *big.Int {
	d.hashMutex.Lock()
	defer d.hashMutex.Unlock()
	d.hashFunc.Reset()
	d.hashFunc.Write([]byte(key))
	hashBytes := d.hashFunc.Sum(nil)
	return new(big.Int).SetBytes(hashBytes)
}

// GetNode returns the node responsible for a given key
func (d *Dynamo) GetNode(key string) *Node {
	hash := d.GetHash(key)
	var closestNode *Node
	var closestHash *big.Int
	for _, node := range d.Nodes {
		if !node.Available {
			continue
		}
		nodeHash := d.GetHash(node.ID)
		if closestNode == nil || nodeHash.Cmp(hash) > 0 && (closestHash == nil || nodeHash.Cmp(closestHash) < 0) {
			closestNode = node
			closestHash = nodeHash
		}
	}
	return closestNode
}

// Put inserts or updates a key-value pair in the Dynamo system
func (d *Dynamo) Put(key, value string) {
	vectorClock := make(map[string]int)
	for _, node := range d.Nodes {
		if !node.Available {
			continue
		}
		vectorClock[node.ID]++
	}
	node := d.GetNode(key)
	if node != nil {
		node.Put(key, value, vectorClock)
	}
}

// Get retrieves a key-value pair from the Dynamo system
func (d *Dynamo) Get(key string) (string, bool) {
	node := d.GetNode(key)
	if node == nil {
		return "", false
	}
	data, exists := node.Get(key)
	return data.Value, exists
}

// HandleNodeRecovery handles recovery of a node
func (d *Dynamo) HandleNodeRecovery(nodeID string) {
	// Logic to handle node recovery and data synchronization
	fmt.Printf("Node %s recovered\n", nodeID)
}

// Main function
func main() {
	// Initialize Dynamo system
	dynamo := NewDynamo(2, 2, 3)
	dynamo.AddNode("Node1")
	dynamo.AddNode("Node2")
	dynamo.AddNode("Node3")
	dynamo.AddNode("Node4")
	dynamo.AddNode("Node5")

	// Example put and get
	dynamo.Put("my-key", "my-value")
	value, found := dynamo.Get("my-key")
	if found {
		fmt.Printf("Key %s has value %s\n", "my-key", value)
	} else {
		fmt.Println("Key not found")
	}

	// Example of node failure and recovery
	dynamo.Nodes["Node2"].Available = false
	dynamo.Put("another-key", "another-value")
	dynamo.Nodes["Node2"].Available = true
	dynamo.HandleNodeRecovery("Node2")
	value, found = dynamo.Get("another-key")
	if found {
		fmt.Printf("Key %s has value %s\n", "another-key", value)
	} else {
		fmt.Println("Key not found")
	}

	// Gossip and failure detection example
	for _, node := range dynamo.Nodes {
		node.Gossip(dynamo.Nodes)
	}
	time.Sleep(1 * time.Second)
	for _, node := range dynamo.Nodes {
		node.CheckFailures(dynamo.Nodes)
	}

	// Example of Merkle tree comparison
	node1 := dynamo.Nodes["Node1"]
	node2 := dynamo.Nodes["Node2"]
	equal, diffNode1, diffNode2 := node1.Merkle.Compare(node2.Merkle)
	if !equal {
		fmt.Printf("Merkle trees are different at nodes %v and %v\n", diffNode1, diffNode2)
	} else {
		fmt.Println("Merkle trees are identical")
	}
}
