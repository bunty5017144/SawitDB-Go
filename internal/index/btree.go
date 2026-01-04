package index

// Simple B-Tree Index for SawitDB
type BTreeNode struct {
	IsLeaf   bool
	Keys     []interface{}
	Values   []interface{} // For leaf nodes: array of record references (or []interface{} if multiple)
	Children []*BTreeNode  // For internal nodes
}

func NewBTreeNode(isLeaf bool) *BTreeNode {
	return &BTreeNode{
		IsLeaf:   isLeaf,
		Keys:     make([]interface{}, 0),
		Values:   make([]interface{}, 0),
		Children: make([]*BTreeNode, 0),
	}
}

type BTreeIndex struct {
	Order    int
	Root     *BTreeNode
	Name     string
	KeyField string
}

func NewBTreeIndex(order int) *BTreeIndex {
	if order == 0 {
		order = 32
	}
	return &BTreeIndex{
		Order: order,
		Root:  NewBTreeNode(true),
	}
}

// Helper for comparison
func compare(a, b interface{}) int {
	switch v1 := a.(type) {
	case int:
		v2, ok := b.(int)
		if !ok {
			// Try float64 conversion if matched type mismatch
			if v2f, ok := b.(float64); ok {
				if float64(v1) < v2f {
					return -1
				}
				if float64(v1) > v2f {
					return 1
				}
				return 0
			}
			return 0 // Should handle error
		}
		if v1 < v2 {
			return -1
		}
		if v1 > v2 {
			return 1
		}
		return 0
	case float64:
		v2, ok := b.(float64)
		if !ok {
			if v2i, ok := b.(int); ok {
				if v1 < float64(v2i) {
					return -1
				}
				if v1 > float64(v2i) {
					return 1
				}
				return 0
			}
			return 0
		}
		if v1 < v2 {
			return -1
		}
		if v1 > v2 {
			return 1
		}
		return 0
	case string:
		v2, ok := b.(string)
		if !ok {
			return 0
		}
		if v1 < v2 {
			return -1
		}
		if v1 > v2 {
			return 1
		}
		return 0
	default:
		return 0
	}
}

func (bt *BTreeIndex) Insert(key interface{}, value interface{}) {
	root := bt.Root
	if len(root.Keys) >= bt.Order {
		newRoot := NewBTreeNode(false)
		newRoot.Children = append(newRoot.Children, bt.Root)
		bt.splitChild(newRoot, 0)
		bt.Root = newRoot
		bt.insertNonFull(newRoot, key, value)
	} else {
		bt.insertNonFull(root, key, value)
	}
}

func (bt *BTreeIndex) insertNonFull(node *BTreeNode, key interface{}, value interface{}) {
	i := len(node.Keys) - 1

	if node.IsLeaf {
		// Insert key-value in sorted order
		// Expand slices
		node.Keys = append(node.Keys, nil)
		node.Values = append(node.Values, nil)

		// Shift
		for i >= 0 && compare(key, node.Keys[i]) < 0 {
			node.Keys[i+1] = node.Keys[i]
			node.Values[i+1] = node.Values[i]
			i--
		}

		node.Keys[i+1] = key
		node.Values[i+1] = value
	} else {
		for i >= 0 && compare(key, node.Keys[i]) < 0 {
			i--
		}
		i++

		if len(node.Children[i].Keys) >= bt.Order {
			bt.splitChild(node, i)
			if compare(key, node.Keys[i]) > 0 {
				i++
			}
		}

		bt.insertNonFull(node.Children[i], key, value)
	}
}

func (bt *BTreeIndex) splitChild(parent *BTreeNode, index int) {
	fullNode := parent.Children[index]
	newNode := NewBTreeNode(fullNode.IsLeaf)
	mid := bt.Order / 2

	// Move half of keys to new node
	// Note: Go slices are references, but we want to cut.
	// fullNode.Keys[mid:]

	// Create copies to be safe or use append
	newNode.Keys = append(newNode.Keys, fullNode.Keys[mid:]...)
	fullNode.Keys = fullNode.Keys[:mid]

	if fullNode.IsLeaf {
		newNode.Values = append(newNode.Values, fullNode.Values[mid:]...)
		fullNode.Values = fullNode.Values[:mid]
	} else {
		newNode.Children = append(newNode.Children, fullNode.Children[mid:]...)
		fullNode.Children = fullNode.Children[:mid]
	}

	// Move middle key up to parent
	// In standard B-Tree, middle key moves up.
	// JS implementation: "newNode.keys = fullNode.keys.splice(mid)" ... "middleKey = newNode.keys.shift()"
	// So conceptually:
	// [0..mid-1] stay in fullNode
	// [mid] moves up
	// [mid+1..end] go to newNode

	// Wait, JS splice(mid) removes elements from mid to end and returns them.
	// So fullNode keeps 0..mid-1. NewNode gets mid..end.
	// Then shift() takes the first of newNode (which was 'mid').

	// So my Go logic above:
	// newNode.Keys gets index 'mid' onwards.
	// Then I need to take the first element of newNode.Keys as middleKey.

	middleKey := newNode.Keys[0]
	newNode.Keys = newNode.Keys[1:]

	if fullNode.IsLeaf {
		// In JS: if leaf, values.shift().
		// This implies the value associated with the promoted key is also removed from the leaf?
		// "node.values.shift()"
		// Usually in B+ Trees, leaves keep all keys. In B-Trees, keys move up.
		// The JS implementation seems to be a B-Tree (values move with keys? or just lost for the pivot?)
		// JS: node.values.shift(). It removes the value associated with the middle key from the leaf.
		// So data for that key is... gone from the leaf?
		// Wait, if it's a B-Tree, internal nodes store keys AND values (or just keys in internal?)
		// JS BTreeNode has values only "For leaf nodes". Children for internal.
		// If a key moves up to an internal node, where does its value go?
		// The JS code doesn't store values in internal nodes!
		// "this.children = [] // For internal nodes" vs "this.values = [] // For leaf nodes"
		// If a key moves up, its value is LOST if it's not stored in internal nodes.
		// BUT the JS code `_insertNonFull`:
		// If leaf: store key and value.
		// If split: Move middle key up. `values.shift()`.
		// If internal node keys don't store values, then searching for that key in internal node won't find the value?
		// JS `_searchNode`:
		// `if (key === node.keys[i])`
		// `if (node.isLeaf) return vals...`
		// `else return _searchNode(node.children[i+1], key)`
		// It SKIPS the match in internal node and goes to right child!
		// This implies the key MUST exist in the right child (or left?) if it's acting as a pivot.
		// But in `splitChild`, the key was REMOVED from the child (it was shifted from newNode).
		// So the key is in Parent, but NOT in children.
		// And search skips Parent match and goes to child.
		// This logic seems flawed in the JS source or I am misreading.
		// `if (key === node.keys[i])`:
		//    If NOT leaf, `return this._searchNode(node.children[i + 1], key);`
		//    The key match is found in internal node. We descend to `i+1`.
		//    Does `children[i+1]` contain the key?
		//    We just removed it from `newNode` (which is `children[i+1]`).
		// So the key is NOT in `children[i+1]`.
		// So `_searchNode` will likely NOT find it in the child?
		// Unless I misread `splice` or something.
		// `newNode.keys` had `mid`...`end`. `shift` removed `mid`.
		// So `mid` is gone from newNode.
		// So `mid` is ONLY in parent.
		// But `_searchNode` ignores match in parent and descends.
		// So `search` returns `[]` (not found) for keys that were promoted?
		// This looks like a bug in the JS implementation provided.
		// However, I must port it AS IS.
		// If the JS version is broken for split keys, so be it?
		// Or maybe `_searchNode` logic finds it later?
		// Wait, if `key > node.keys[i]` -> `i++`.
		// If `key == node.keys[i]`, we hit the block.
		// `else` (internal): `_searchNode(node.children[i+1], key)`.
		// It essentially says "If equal, go right".
		// Maybe the key is duplicated?
		// But `shift()` removes it.
		// Okay, I will strictly follow the JS logic. "Garbage in, garbage out" or maybe there's a trick I don't see.
		// Actually, if I look at `delete` or others, maybe it expects it.
		// Let's just blindly port the logic.

		newNode.Values = newNode.Values[1:]
	}

	// Insert into parent
	// Insert middleKey at index
	// Insert newNode at index+1

	// Expand parent keys/children
	parent.Keys = append(parent.Keys, nil)
	copy(parent.Keys[index+1:], parent.Keys[index:])
	parent.Keys[index] = middleKey

	parent.Children = append(parent.Children, nil)
	copy(parent.Children[index+2:], parent.Children[index+1:])
	parent.Children[index+1] = newNode
}

func (bt *BTreeIndex) Search(key interface{}) []interface{} {
	return bt.searchNode(bt.Root, key)
}

func (bt *BTreeIndex) searchNode(node *BTreeNode, key interface{}) []interface{} {
	i := 0
	for i < len(node.Keys) && compare(key, node.Keys[i]) > 0 {
		i++
	}

	if i < len(node.Keys) && compare(key, node.Keys[i]) == 0 {
		if node.IsLeaf {
			// In JS: return Array.isArray(val) ? val : [val]
			// We store val as interface{}. If it's a slice, expand?
			// The JS insert puts `data` (whole object) as value.
			// So it's likely a single objects.
			// However `node.values` in JS is array of references.
			// Wait, the JS `insert` logic: `node.values[i+1] = value`.
			// It overwrites? No, it shifts then sets. So one value per key.
			// But BTree usually handles duplicates?
			// JS implementation seems to overwrite/store one value per unique key instance in that sorted slot?
			// Actually `insert` finds position. equality behavior?
			// `while (i >= 0 && key < node.keys[i])`.
			// If key == node.keys[i], it stops?
			// No, `key < ...` is false. Loop stops.
			// It inserts at `i+1`.
			// So it inserts AFTER the equal key.
			// So it supports duplicates.
			// So `Search` finding the *first* match:
			// `while ... key > node.keys[i]`. Stops at equal.
			// Returns `node.values[i]`.
			// This effectively returns the *first* one found in that node.
			// What about others? Unclear. JS logic says `return Array.isArray...` implying value could be array?
			// But `insert` puts single value.
			// Unless `value` passed to insert IS an array?
			// In `WowoEngine`: `index.insert(data[field], data);` -> `data` is an object.
			// So it returns `[obj]`.
			val := node.Values[i]
			return []interface{}{val}
		} else {
			return bt.searchNode(node.Children[i+1], key)
		}
	}

	if node.IsLeaf {
		return []interface{}{}
	}

	return bt.searchNode(node.Children[i], key)
}

func (bt *BTreeIndex) Range(min, max interface{}) []interface{} {
	results := make([]interface{}, 0)
	bt.rangeSearch(bt.Root, min, max, &results)
	return results
}

func (bt *BTreeIndex) rangeSearch(node *BTreeNode, min, max interface{}, results *[]interface{}) {
	i := 0
	for i < len(node.Keys) {
		if node.IsLeaf {
			k := node.Keys[i]
			cMin := compare(k, min)
			cMax := compare(k, max)
			if cMin >= 0 && cMax <= 0 {
				val := node.Values[i]
				// Append val
				*results = append(*results, val)
			}
			i++
		} else {
			k := node.Keys[i]
			if compare(k, min) > 0 {
				bt.rangeSearch(node.Children[i], min, max, results)
			}
			i++
		}
	}

	if !node.IsLeaf && len(node.Children) > i {
		bt.rangeSearch(node.Children[i], min, max, results)
	}
}

func (bt *BTreeIndex) Stats() map[string]interface{} {
	nodeCount := 0
	leafCount := 0
	keyCount := 0
	maxDepth := 0

	var traverse func(*BTreeNode, int)
	traverse = func(node *BTreeNode, depth int) {
		nodeCount++
		keyCount += len(node.Keys)
		if depth > maxDepth {
			maxDepth = depth
		}

		if node.IsLeaf {
			leafCount++
		} else {
			for _, child := range node.Children {
				traverse(child, depth+1)
			}
		}
	}

	traverse(bt.Root, 0)

	return map[string]interface{}{
		"name":      bt.Name,
		"keyField":  bt.KeyField,
		"nodeCount": nodeCount,
		"leafCount": leafCount,
		"keyCount":  keyCount,
		"maxDepth":  maxDepth,
		"order":     bt.Order,
	}
}

func (bt *BTreeIndex) Clear() {
	bt.Root = NewBTreeNode(true)
}
