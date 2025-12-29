// Created by Yanjunhui

package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/monolite/monodb/internal/failpoint"
)

// BTreeOrder B+Tree 的阶数（每个节点最多存储的键数）
// 注意：实际分裂判定由字节大小决定，BTreeOrder 仅作为上限参考
const BTreeOrder = 50

// 索引键值大小限制
// 这些限制确保单个键/值不会超过页面容量，避免节点无法序列化
const (
	// MaxIndexKeyBytes 单个索引键的最大字节数
	// 考虑到节点需要存储多个键和元数据，单键不应超过可用空间的 1/4
	MaxIndexKeyBytes = MaxPageData / 4 // 约 1KB

	// MaxIndexValueBytes 单个索引值的最大字节数
	// 通常是 RecordId（6字节），但为扩展性预留更多空间
	MaxIndexValueBytes = 256

	// MaxIndexEntryBytes 单条索引记录（key + value）的最大字节数
	MaxIndexEntryBytes = MaxIndexKeyBytes + MaxIndexValueBytes

	// BTreeNodeHeaderSize 节点头部固定大小
	// IsLeaf(1) + KeyCount(2) + Next(4) + Prev(4) = 11 bytes
	BTreeNodeHeaderSize = 11

	// BTreeNodeMaxBytes 节点数据区最大字节数（用于字节驱动分裂判定）
	// 预留一些空间给页面头部和对齐
	BTreeNodeMaxBytes = MaxPageData - 64

	// BTreeNodeSplitThreshold 触发分裂的字节阈值
	// 当节点大小超过此值时需要分裂
	BTreeNodeSplitThreshold = BTreeNodeMaxBytes * 3 / 4
)

// ErrIndexKeyTooLarge 索引键过大错误
var ErrIndexKeyTooLarge = fmt.Errorf("index key exceeds maximum size (%d bytes)", MaxIndexKeyBytes)

// ErrIndexValueTooLarge 索引值过大错误
var ErrIndexValueTooLarge = fmt.Errorf("index value exceeds maximum size (%d bytes)", MaxIndexValueBytes)

// BTreeNode 表示 B+Tree 的一个节点
type BTreeNode struct {
	PageId   PageId   // 节点对应的页面 ID
	IsLeaf   bool     // 是否是叶子节点
	KeyCount int      // 当前键的数量
	Keys     [][]byte // 键列表
	Values   [][]byte // 值列表（仅叶子节点使用，存储 RecordId）
	Children []PageId // 子节点页面 ID（仅内部节点使用）
	Next     PageId   // 下一个叶子节点（仅叶子节点使用，用于范围查询）
	Prev     PageId   // 上一个叶子节点
}

// ByteSize 计算节点序列化后的字节大小
// 用于字节驱动的分裂判定
func (n *BTreeNode) ByteSize() int {
	size := BTreeNodeHeaderSize // 头部固定大小

	// 键的大小：每个键有 2 字节长度前缀 + 键数据
	for _, key := range n.Keys {
		size += 2 + len(key)
	}

	if n.IsLeaf {
		// 叶子节点：每个值有 2 字节长度前缀 + 值数据
		for _, value := range n.Values {
			size += 2 + len(value)
		}
	} else {
		// 内部节点：每个子节点 ID 4 字节
		size += len(n.Children) * 4
	}

	return size
}

// NeedsSplit 检查节点是否需要分裂（字节驱动判定）
func (n *BTreeNode) NeedsSplit() bool {
	return n.ByteSize() > BTreeNodeSplitThreshold
}

// CanAccommodate 检查节点是否能容纳新的键值对
func (n *BTreeNode) CanAccommodate(key, value []byte) bool {
	additionalSize := 2 + len(key) // 键
	if n.IsLeaf {
		additionalSize += 2 + len(value) // 值
	} else {
		additionalSize += 4 // 子节点 ID
	}
	return n.ByteSize()+additionalSize <= BTreeNodeMaxBytes
}

// RecordId 记录标识符
type RecordId struct {
	PageId    PageId // 页面 ID
	SlotIndex uint16 // 槽索引
}

// RecordIdSize RecordId 的字节大小
const RecordIdSize = 6

// MarshalRecordId 序列化 RecordId
func MarshalRecordId(rid RecordId) []byte {
	buf := make([]byte, RecordIdSize)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(rid.PageId))
	binary.LittleEndian.PutUint16(buf[4:6], rid.SlotIndex)
	return buf
}

// UnmarshalRecordId 反序列化 RecordId
func UnmarshalRecordId(data []byte) RecordId {
	return RecordId{
		PageId:    PageId(binary.LittleEndian.Uint32(data[0:4])),
		SlotIndex: binary.LittleEndian.Uint16(data[4:6]),
	}
}

// BTree B+Tree 索引结构
type BTree struct {
	pager    *Pager
	rootPage PageId
	name     string
	unique   bool
}

// NewBTree 创建一个新的 B+Tree
func NewBTree(pager *Pager, name string, unique bool) (*BTree, error) {
	// 分配根节点页面
	rootPage, err := pager.AllocatePage(PageTypeIndex)
	if err != nil {
		return nil, err
	}

	// 初始化为空的叶子节点
	root := &BTreeNode{
		PageId:   rootPage.ID(),
		IsLeaf:   true,
		KeyCount: 0,
		Keys:     make([][]byte, 0),
		Values:   make([][]byte, 0),
		Children: nil,
		Next:     0,
		Prev:     0,
	}

	tree := &BTree{
		pager:    pager,
		rootPage: rootPage.ID(),
		name:     name,
		unique:   unique,
	}

	if err := tree.writeNode(root); err != nil {
		return nil, err
	}

	return tree, nil
}

// OpenBTree 打开已存在的 B+Tree
func OpenBTree(pager *Pager, rootPage PageId, name string, unique bool) *BTree {
	return &BTree{
		pager:    pager,
		rootPage: rootPage,
		name:     name,
		unique:   unique,
	}
}

// RootPage 返回根页面 ID
func (t *BTree) RootPage() PageId {
	return t.rootPage
}

// Name 返回索引名称
func (t *BTree) Name() string {
	return t.name
}

// Insert 插入键值对
// 【BUG-007 修复】对于唯一索引，在叶子节点内部进行原子检查，避免 TOCTOU
func (t *BTree) Insert(key []byte, value []byte) error {
	// 【FAILPOINT】用于测试 B+Tree 插入失败场景
	if err := failpoint.Hit("btree.insert"); err != nil {
		return fmt.Errorf("failpoint: btree.insert: %w", err)
	}

	// 【前置校验】检查键值大小是否超限
	// 这确保了超长键会立即返回明确错误，而不是在节点序列化时才失败
	if len(key) > MaxIndexKeyBytes {
		return fmt.Errorf("index key too large: %d bytes (max: %d)", len(key), MaxIndexKeyBytes)
	}
	if len(value) > MaxIndexValueBytes {
		return fmt.Errorf("index value too large: %d bytes (max: %d)", len(value), MaxIndexValueBytes)
	}

	root, err := t.readNode(t.rootPage)
	if err != nil {
		return err
	}

	// 【BUG-007 修复】移除预先的 Search 检查，改为在 insertNonFull 中原子检查
	// 这避免了 TOCTOU 漏洞

	// 如果根节点满了，需要分裂
	if root.KeyCount >= BTreeOrder-1 {
		// 创建新的根节点
		newRootPage, err := t.pager.AllocatePage(PageTypeIndex)
		if err != nil {
			return err
		}

		newRoot := &BTreeNode{
			PageId:   newRootPage.ID(),
			IsLeaf:   false,
			KeyCount: 0,
			Keys:     make([][]byte, 0),
			Values:   nil,
			Children: []PageId{root.PageId},
			Next:     0,
			Prev:     0,
		}

		t.rootPage = newRoot.PageId

		// 分裂旧根节点
		if err := t.splitChild(newRoot, 0); err != nil {
			return err
		}

		// 在新根节点中插入
		return t.insertNonFull(newRoot, key, value)
	}

	return t.insertNonFull(root, key, value)
}

// insertNonFull 在非满节点中插入
// 【BUG-007 修复】对于唯一索引，在叶子节点中原子检查并插入
func (t *BTree) insertNonFull(node *BTreeNode, key []byte, value []byte) error {
	i := node.KeyCount - 1

	if node.IsLeaf {
		// 在叶子节点中找到插入位置
		for i >= 0 && bytes.Compare(key, node.Keys[i]) < 0 {
			i--
		}

		// 【BUG-007 修复】对于唯一索引，在插入位置附近检查是否已存在相同键
		if t.unique {
			// 检查插入位置右边的键
			checkPos := i + 1
			if checkPos < node.KeyCount && bytes.Equal(key, node.Keys[checkPos]) {
				return fmt.Errorf("duplicate key")
			}
			// 检查插入位置左边的键（即 i 位置）
			if i >= 0 && bytes.Equal(key, node.Keys[i]) {
				return fmt.Errorf("duplicate key")
			}
		}

		i++

		// 插入键值对
		node.Keys = insertAt(node.Keys, i, key)
		node.Values = insertAt(node.Values, i, value)
		node.KeyCount++

		return t.writeNode(node)
	}

	// 内部节点：找到子节点
	for i >= 0 && bytes.Compare(key, node.Keys[i]) < 0 {
		i--
	}
	i++

	child, err := t.readNode(node.Children[i])
	if err != nil {
		return err
	}

	// 如果子节点满了，先分裂
	if child.KeyCount >= BTreeOrder-1 {
		if err := t.splitChild(node, i); err != nil {
			return err
		}

		// 决定走哪个子节点
		if bytes.Compare(key, node.Keys[i]) > 0 {
			i++
		}
		child, err = t.readNode(node.Children[i])
		if err != nil {
			return err
		}
	}

	return t.insertNonFull(child, key, value)
}

// splitChild 分裂子节点
// 使用字节驱动的分裂点：找到使左右两边字节大小尽量均衡的分裂点
func (t *BTree) splitChild(parent *BTreeNode, index int) error {
	child, err := t.readNode(parent.Children[index])
	if err != nil {
		return err
	}

	// 创建新节点页面
	newPage, err := t.pager.AllocatePage(PageTypeIndex)
	if err != nil {
		return err
	}

	// 字节驱动的分裂点计算：找到使左右两边字节大小尽量均衡的点
	// 目标是让分裂后两个节点的大小都不超过 BTreeNodeSplitThreshold
	mid := findByteDrivenSplitPoint(child)

	newNode := &BTreeNode{
		PageId:   newPage.ID(),
		IsLeaf:   child.IsLeaf,
		Keys:     make([][]byte, 0),
		Values:   make([][]byte, 0),
		Children: nil,
		Next:     child.Next,
		Prev:     child.PageId,
	}

	// 用于收集需要写入的节点（原子更新策略）
	nodesToWrite := make([]*BTreeNode, 0, 4)

	if child.IsLeaf {
		// 叶子节点：复制右半部分
		newNode.Keys = append(newNode.Keys, child.Keys[mid:]...)
		newNode.Values = append(newNode.Values, child.Values[mid:]...)
		newNode.KeyCount = len(newNode.Keys)

		// 更新链表：需要更新 nextNode.Prev
		child.Next = newNode.PageId
		if newNode.Next != 0 {
			nextNode, err := t.readNode(newNode.Next)
			if err != nil {
				// 链表更新失败，需要回滚分配的页面
				t.pager.FreePage(newPage.ID())
				return fmt.Errorf("failed to read next node for leaf chain update: %w", err)
			}
			nextNode.Prev = newNode.PageId
			nodesToWrite = append(nodesToWrite, nextNode)
		}

		// 提升中间键到父节点（叶子节点保留一份）
		midKey := child.Keys[mid]

		child.Keys = child.Keys[:mid]
		child.Values = child.Values[:mid]
		child.KeyCount = len(child.Keys)

		// 更新父节点
		parent.Keys = insertAt(parent.Keys, index, midKey)
		parent.Children = insertPageIdAt(parent.Children, index+1, newNode.PageId)
		parent.KeyCount++
	} else {
		// 内部节点
		midKey := child.Keys[mid]

		newNode.Keys = append(newNode.Keys, child.Keys[mid+1:]...)
		newNode.Children = append([]PageId{}, child.Children[mid+1:]...)
		newNode.KeyCount = len(newNode.Keys)

		child.Keys = child.Keys[:mid]
		child.Children = child.Children[:mid+1]
		child.KeyCount = len(child.Keys)

		parent.Keys = insertAt(parent.Keys, index, midKey)
		parent.Children = insertPageIdAt(parent.Children, index+1, newNode.PageId)
		parent.KeyCount++
	}

	// 添加主要需要写入的节点
	nodesToWrite = append(nodesToWrite, child, newNode, parent)

	// 原子写入所有修改的节点
	// 如果任何写入失败，返回错误（调用者需要处理不一致状态）
	for _, node := range nodesToWrite {
		if err := t.writeNode(node); err != nil {
			return fmt.Errorf("splitChild failed to write node %d: %w", node.PageId, err)
		}
	}

	return nil
}

// findByteDrivenSplitPoint 计算字节驱动的分裂点
// 返回分裂点索引，使得左边节点（包含 [0, mid-1]）和右边节点（包含 [mid, end]）的大小尽量均衡
func findByteDrivenSplitPoint(node *BTreeNode) int {
	if node.KeyCount <= 1 {
		return 0
	}

	// 计算总大小
	totalSize := node.ByteSize()
	targetSize := totalSize / 2

	// 从头开始累积，找到最接近目标大小的分裂点
	leftSize := BTreeNodeHeaderSize
	bestMid := node.KeyCount / 2 // 默认使用中点

	for i := 0; i < node.KeyCount; i++ {
		leftSize += 2 + len(node.Keys[i])
		if node.IsLeaf {
			leftSize += 2 + len(node.Values[i])
		} else if i < len(node.Children) {
			leftSize += 4
		}

		// 检查是否接近目标
		if leftSize >= targetSize {
			// 确保分裂点至少在 1 和 KeyCount-1 之间
			if i < 1 {
				bestMid = 1
			} else if i >= node.KeyCount-1 {
				bestMid = node.KeyCount - 1
			} else {
				bestMid = i
			}
			break
		}
	}

	// 确保分裂点有效
	if bestMid < 1 {
		bestMid = 1
	}
	if bestMid >= node.KeyCount {
		bestMid = node.KeyCount - 1
	}

	return bestMid
}

// Search 搜索键
func (t *BTree) Search(key []byte) ([]byte, error) {
	node, err := t.readNode(t.rootPage)
	if err != nil {
		return nil, err
	}

	for !node.IsLeaf {
		i := 0
		for i < node.KeyCount && bytes.Compare(key, node.Keys[i]) >= 0 {
			i++
		}
		node, err = t.readNode(node.Children[i])
		if err != nil {
			return nil, err
		}
	}

	// 在叶子节点中查找
	for i := 0; i < node.KeyCount; i++ {
		cmp := bytes.Compare(key, node.Keys[i])
		if cmp == 0 {
			return node.Values[i], nil
		}
		if cmp < 0 {
			break
		}
	}

	return nil, nil // 未找到
}

// SearchRange 范围搜索
func (t *BTree) SearchRange(minKey, maxKey []byte, includeMin, includeMax bool) ([][]byte, error) {
	return t.SearchRangeLimit(minKey, maxKey, includeMin, includeMax, -1)
}

// SearchRangeLimit 带限制的范围搜索
// limit < 0 表示不限制，返回所有匹配的记录
// limit >= 0 表示最多返回 limit 条记录
func (t *BTree) SearchRangeLimit(minKey, maxKey []byte, includeMin, includeMax bool, limit int) ([][]byte, error) {
	results := make([][]byte, 0)

	// 找到起始叶子节点
	node, err := t.readNode(t.rootPage)
	if err != nil {
		return nil, err
	}

	for !node.IsLeaf {
		i := 0
		for i < node.KeyCount && bytes.Compare(minKey, node.Keys[i]) >= 0 {
			i++
		}
		node, err = t.readNode(node.Children[i])
		if err != nil {
			return nil, err
		}
	}

	// 遍历叶子节点链表
	for node != nil {
		for i := 0; i < node.KeyCount; i++ {
			// 检查是否已达到限制
			if limit >= 0 && len(results) >= limit {
				return results, nil
			}

			key := node.Keys[i]

			// 检查下界
			if minKey != nil {
				cmp := bytes.Compare(key, minKey)
				if cmp < 0 || (!includeMin && cmp == 0) {
					continue
				}
			}

			// 检查上界
			if maxKey != nil {
				cmp := bytes.Compare(key, maxKey)
				if cmp > 0 || (!includeMax && cmp == 0) {
					return results, nil
				}
			}

			results = append(results, node.Values[i])
		}

		// 移动到下一个叶子节点
		if node.Next == 0 {
			break
		}
		node, err = t.readNode(node.Next)
		if err != nil {
			return nil, err
		}
	}

	return results, nil
}

// MinKeys 返回节点最小键数（除根节点外）
func MinKeys() int {
	return (BTreeOrder - 1) / 2
}

// SearchRangeLimitReverse 从尾部反向获取 limit 条记录
// 用于分段采样验证，获取索引尾部的记录
func (t *BTree) SearchRangeLimitReverse(limit int) ([][]byte, error) {
	if limit <= 0 {
		return nil, nil
	}

	// 找到最后一个叶子节点
	node, err := t.readNode(t.rootPage)
	if err != nil {
		return nil, err
	}

	for !node.IsLeaf {
		// 走到最右边的子节点
		node, err = t.readNode(node.Children[node.KeyCount])
		if err != nil {
			return nil, err
		}
	}

	// 从最后一个叶子节点反向遍历
	results := make([][]byte, 0, limit)
	for node != nil && len(results) < limit {
		// 从节点末尾向前遍历
		for i := node.KeyCount - 1; i >= 0 && len(results) < limit; i-- {
			results = append(results, node.Values[i])
		}

		// 移动到前一个叶子节点
		if node.Prev == 0 {
			break
		}
		node, err = t.readNode(node.Prev)
		if err != nil {
			return nil, err
		}
	}

	return results, nil
}

// SearchRangeLimitSkip 跳过 skip 条记录后获取 limit 条记录
// 用于分段采样验证，获取索引中间位置的记录
func (t *BTree) SearchRangeLimitSkip(skip, limit int) ([][]byte, error) {
	if limit <= 0 {
		return nil, nil
	}

	results := make([][]byte, 0, limit)
	skipped := 0

	// 找到起始叶子节点
	node, err := t.readNode(t.rootPage)
	if err != nil {
		return nil, err
	}

	for !node.IsLeaf {
		node, err = t.readNode(node.Children[0])
		if err != nil {
			return nil, err
		}
	}

	// 遍历叶子节点链表
	for node != nil {
		for i := 0; i < node.KeyCount; i++ {
			// 先跳过 skip 条
			if skipped < skip {
				skipped++
				continue
			}

			// 检查是否已达到限制
			if len(results) >= limit {
				return results, nil
			}

			results = append(results, node.Values[i])
		}

		// 移动到下一个叶子节点
		if node.Next == 0 {
			break
		}
		node, err = t.readNode(node.Next)
		if err != nil {
			return nil, err
		}
	}

	return results, nil
}

// Delete 删除键（带节点合并）
func (t *BTree) Delete(key []byte) error {
	// 【FAILPOINT】用于测试 B+Tree 删除失败场景
	if err := failpoint.Hit("btree.delete"); err != nil {
		return fmt.Errorf("failpoint: btree.delete: %w", err)
	}

	return t.deleteInternal(t.rootPage, key, nil, -1)
}

// deleteInternal 递归删除
func (t *BTree) deleteInternal(nodeId PageId, key []byte, parent *BTreeNode, childIndex int) error {
	node, err := t.readNode(nodeId)
	if err != nil {
		return err
	}

	if node.IsLeaf {
		return t.deleteFromLeaf(node, key, parent, childIndex)
	}

	// 内部节点：找到子节点
	i := 0
	for i < node.KeyCount && bytes.Compare(key, node.Keys[i]) >= 0 {
		i++
	}

	// 确保索引有效
	if i >= len(node.Children) {
		i = len(node.Children) - 1
	}
	if i < 0 {
		return nil
	}

	// 递归删除
	if err := t.deleteInternal(node.Children[i], key, node, i); err != nil {
		return err
	}

	// 重新读取节点（可能已被修改）
	node, err = t.readNode(nodeId)
	if err != nil {
		return err
	}

	// 检查子节点是否需要修复（索引可能已变化）
	if i < len(node.Children) {
		return t.fixAfterDelete(node, i)
	}

	return nil
}

// deleteFromLeaf 从叶子节点删除
func (t *BTree) deleteFromLeaf(node *BTreeNode, key []byte, parent *BTreeNode, childIndex int) error {
	// 查找键
	found := -1
	for i := 0; i < node.KeyCount; i++ {
		if bytes.Equal(key, node.Keys[i]) {
			found = i
			break
		}
	}

	if found == -1 {
		return nil // 键不存在，静默成功
	}

	// 删除键值对
	node.Keys = append(node.Keys[:found], node.Keys[found+1:]...)
	node.Values = append(node.Values[:found], node.Values[found+1:]...)
	node.KeyCount--

	if err := t.writeNode(node); err != nil {
		return err
	}

	// 检查是否需要修复（根节点不需要）
	if parent != nil && node.KeyCount < MinKeys() {
		return t.fixUnderflow(node, parent, childIndex)
	}

	return nil
}

// fixAfterDelete 删除后修复节点
func (t *BTree) fixAfterDelete(parent *BTreeNode, childIndex int) error {
	// 检查索引有效性
	if childIndex < 0 || childIndex >= len(parent.Children) {
		return nil
	}

	child, err := t.readNode(parent.Children[childIndex])
	if err != nil {
		return err
	}

	// 如果子节点键数足够，无需修复
	if child.KeyCount >= MinKeys() {
		return nil
	}

	// 检查是否为根节点且已空
	if parent.PageId == t.rootPage && parent.KeyCount == 0 {
		// 如果根节点变空，提升唯一的子节点为新根
		if len(parent.Children) > 0 {
			t.rootPage = parent.Children[0]
		}
		return nil
	}

	return t.fixUnderflow(child, parent, childIndex)
}

// fixUnderflow 修复下溢节点
func (t *BTree) fixUnderflow(node *BTreeNode, parent *BTreeNode, childIndex int) error {
	// 尝试从左兄弟借键
	if childIndex > 0 {
		leftSibling, err := t.readNode(parent.Children[childIndex-1])
		if err == nil && leftSibling.KeyCount > MinKeys() {
			return t.borrowFromLeft(node, leftSibling, parent, childIndex)
		}
	}

	// 尝试从右兄弟借键
	if childIndex < len(parent.Children)-1 {
		rightSibling, err := t.readNode(parent.Children[childIndex+1])
		if err == nil && rightSibling.KeyCount > MinKeys() {
			return t.borrowFromRight(node, rightSibling, parent, childIndex)
		}
	}

	// 无法借键，需要合并
	if childIndex > 0 {
		// 与左兄弟合并
		leftSibling, err := t.readNode(parent.Children[childIndex-1])
		if err != nil {
			return err
		}
		return t.mergeNodes(leftSibling, node, parent, childIndex-1)
	} else if childIndex < len(parent.Children)-1 {
		// 与右兄弟合并
		rightSibling, err := t.readNode(parent.Children[childIndex+1])
		if err != nil {
			return err
		}
		return t.mergeNodes(node, rightSibling, parent, childIndex)
	}

	return nil
}

// borrowFromLeft 从左兄弟借一个键
func (t *BTree) borrowFromLeft(node, leftSibling *BTreeNode, parent *BTreeNode, childIndex int) error {
	if node.IsLeaf {
		// 叶子节点：直接借
		borrowedKey := leftSibling.Keys[leftSibling.KeyCount-1]
		borrowedVal := leftSibling.Values[leftSibling.KeyCount-1]

		// 移除左兄弟的最后一个键
		leftSibling.Keys = leftSibling.Keys[:leftSibling.KeyCount-1]
		leftSibling.Values = leftSibling.Values[:leftSibling.KeyCount-1]
		leftSibling.KeyCount--

		// 添加到当前节点开头
		node.Keys = append([][]byte{borrowedKey}, node.Keys...)
		node.Values = append([][]byte{borrowedVal}, node.Values...)
		node.KeyCount++

		// 更新父节点的分隔键
		parent.Keys[childIndex-1] = node.Keys[0]
	} else {
		// 内部节点：需要旋转
		separatorKey := parent.Keys[childIndex-1]
		borrowedChild := leftSibling.Children[len(leftSibling.Children)-1]

		// 移除左兄弟的最后一个键和子节点
		newParentKey := leftSibling.Keys[leftSibling.KeyCount-1]
		leftSibling.Keys = leftSibling.Keys[:leftSibling.KeyCount-1]
		leftSibling.Children = leftSibling.Children[:len(leftSibling.Children)-1]
		leftSibling.KeyCount--

		// 添加分隔键和子节点到当前节点开头
		node.Keys = append([][]byte{separatorKey}, node.Keys...)
		node.Children = append([]PageId{borrowedChild}, node.Children...)
		node.KeyCount++

		// 更新父节点的分隔键
		parent.Keys[childIndex-1] = newParentKey
	}

	// 写入所有修改的节点
	if err := t.writeNode(leftSibling); err != nil {
		return err
	}
	if err := t.writeNode(node); err != nil {
		return err
	}
	return t.writeNode(parent)
}

// borrowFromRight 从右兄弟借一个键
func (t *BTree) borrowFromRight(node, rightSibling *BTreeNode, parent *BTreeNode, childIndex int) error {
	if node.IsLeaf {
		// 叶子节点：直接借
		borrowedKey := rightSibling.Keys[0]
		borrowedVal := rightSibling.Values[0]

		// 移除右兄弟的第一个键
		rightSibling.Keys = rightSibling.Keys[1:]
		rightSibling.Values = rightSibling.Values[1:]
		rightSibling.KeyCount--

		// 添加到当前节点末尾
		node.Keys = append(node.Keys, borrowedKey)
		node.Values = append(node.Values, borrowedVal)
		node.KeyCount++

		// 更新父节点的分隔键
		parent.Keys[childIndex] = rightSibling.Keys[0]
	} else {
		// 内部节点：需要旋转
		separatorKey := parent.Keys[childIndex]
		borrowedChild := rightSibling.Children[0]

		// 移除右兄弟的第一个键和子节点
		newParentKey := rightSibling.Keys[0]
		rightSibling.Keys = rightSibling.Keys[1:]
		rightSibling.Children = rightSibling.Children[1:]
		rightSibling.KeyCount--

		// 添加分隔键和子节点到当前节点末尾
		node.Keys = append(node.Keys, separatorKey)
		node.Children = append(node.Children, borrowedChild)
		node.KeyCount++

		// 更新父节点的分隔键
		parent.Keys[childIndex] = newParentKey
	}

	// 写入所有修改的节点
	if err := t.writeNode(rightSibling); err != nil {
		return err
	}
	if err := t.writeNode(node); err != nil {
		return err
	}
	return t.writeNode(parent)
}

// mergeNodes 合并两个节点
// 所有 writeNode 错误都上抛，确保一致性
func (t *BTree) mergeNodes(left, right *BTreeNode, parent *BTreeNode, separatorIndex int) error {
	// 用于收集需要写入的节点（原子更新策略）
	nodesToWrite := make([]*BTreeNode, 0, 3)

	if left.IsLeaf {
		// 叶子节点合并
		left.Keys = append(left.Keys, right.Keys...)
		left.Values = append(left.Values, right.Values...)
		left.KeyCount = len(left.Keys)

		// 更新叶子链表
		left.Next = right.Next
		if right.Next != 0 {
			nextNode, err := t.readNode(right.Next)
			if err != nil {
				return fmt.Errorf("mergeNodes failed to read next node: %w", err)
			}
			nextNode.Prev = left.PageId
			nodesToWrite = append(nodesToWrite, nextNode)
		}
	} else {
		// 内部节点合并：需要下推分隔键
		separatorKey := parent.Keys[separatorIndex]
		left.Keys = append(left.Keys, separatorKey)
		left.Keys = append(left.Keys, right.Keys...)
		left.Children = append(left.Children, right.Children...)
		left.KeyCount = len(left.Keys)
	}

	// 从父节点移除分隔键和右子节点指针
	parent.Keys = append(parent.Keys[:separatorIndex], parent.Keys[separatorIndex+1:]...)
	parent.Children = append(parent.Children[:separatorIndex+1], parent.Children[separatorIndex+2:]...)
	parent.KeyCount--

	// 检查是否需要更新根节点
	if parent.PageId == t.rootPage && parent.KeyCount == 0 {
		t.rootPage = left.PageId
	}

	// 添加主要需要写入的节点
	nodesToWrite = append(nodesToWrite, left, parent)

	// 原子写入所有修改的节点
	for _, node := range nodesToWrite {
		if err := t.writeNode(node); err != nil {
			return fmt.Errorf("mergeNodes failed to write node %d: %w", node.PageId, err)
		}
	}

	// 最后释放右节点页面（在所有写入成功后）
	if err := t.pager.FreePage(right.PageId); err != nil {
		return fmt.Errorf("mergeNodes failed to free right node page %d: %w", right.PageId, err)
	}

	return nil
}

// readNode 从页面读取节点
// 【BUG-012 修复】增加一致性验证，检测损坏的节点数据
func (t *BTree) readNode(pageId PageId) (*BTreeNode, error) {
	page, err := t.pager.ReadPage(pageId)
	if err != nil {
		return nil, err
	}

	data := page.Data()
	if len(data) < 10 {
		return nil, fmt.Errorf("invalid btree node page")
	}

	node := &BTreeNode{
		PageId: pageId,
	}

	pos := 0

	// IsLeaf (1 byte)
	node.IsLeaf = data[pos] != 0
	pos++

	// KeyCount (2 bytes)
	node.KeyCount = int(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2

	// Next (4 bytes)
	node.Next = PageId(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	// Prev (4 bytes)
	node.Prev = PageId(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4

	// 读取键
	node.Keys = make([][]byte, 0, node.KeyCount)
	for i := 0; i < node.KeyCount; i++ {
		if pos+2 > len(data) {
			break
		}
		keyLen := int(binary.LittleEndian.Uint16(data[pos:]))
		pos += 2
		if pos+keyLen > len(data) {
			break
		}
		key := make([]byte, keyLen)
		copy(key, data[pos:pos+keyLen])
		node.Keys = append(node.Keys, key)
		pos += keyLen
	}

	if node.IsLeaf {
		// 读取值
		node.Values = make([][]byte, 0, node.KeyCount)
		for i := 0; i < node.KeyCount; i++ {
			if pos+2 > len(data) {
				break
			}
			valLen := int(binary.LittleEndian.Uint16(data[pos:]))
			pos += 2
			if pos+valLen > len(data) {
				break
			}
			val := make([]byte, valLen)
			copy(val, data[pos:pos+valLen])
			node.Values = append(node.Values, val)
			pos += valLen
		}
	} else {
		// 读取子节点指针
		childCount := node.KeyCount + 1
		node.Children = make([]PageId, 0, childCount)
		for i := 0; i < childCount; i++ {
			if pos+4 > len(data) {
				break
			}
			child := PageId(binary.LittleEndian.Uint32(data[pos:]))
			node.Children = append(node.Children, child)
			pos += 4
		}
	}

	// 【BUG-012 修复】一致性验证
	if len(node.Keys) != node.KeyCount {
		return nil, fmt.Errorf("corrupted node %d: key count mismatch (header=%d, actual=%d)",
			pageId, node.KeyCount, len(node.Keys))
	}

	if node.IsLeaf {
		if len(node.Values) != node.KeyCount {
			return nil, fmt.Errorf("corrupted leaf node %d: value count mismatch (keys=%d, values=%d)",
				pageId, node.KeyCount, len(node.Values))
		}
	} else {
		expectedChildren := node.KeyCount + 1
		if len(node.Children) != expectedChildren {
			return nil, fmt.Errorf("corrupted internal node %d: child count mismatch (expected=%d, actual=%d)",
				pageId, expectedChildren, len(node.Children))
		}
	}

	return node, nil
}

// writeNode 将节点写入页面
func (t *BTree) writeNode(node *BTreeNode) error {
	data := make([]byte, MaxPageData)
	pos := 0
	ensure := func(need int) error {
		if need < 0 {
			return fmt.Errorf("invalid write size: %d", need)
		}
		if pos+need > len(data) {
			return fmt.Errorf("btree node %d too large for page: pos=%d need=%d max=%d", node.PageId, pos, need, len(data))
		}
		return nil
	}

	// IsLeaf
	if err := ensure(1); err != nil {
		return err
	}
	if node.IsLeaf {
		data[pos] = 1
	} else {
		data[pos] = 0
	}
	pos++

	// KeyCount
	if err := ensure(2); err != nil {
		return err
	}
	binary.LittleEndian.PutUint16(data[pos:], uint16(node.KeyCount))
	pos += 2

	// Next
	if err := ensure(4); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(data[pos:], uint32(node.Next))
	pos += 4

	// Prev
	if err := ensure(4); err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(data[pos:], uint32(node.Prev))
	pos += 4

	// 写入键
	for _, key := range node.Keys {
		if err := ensure(2 + len(key)); err != nil {
			return err
		}
		binary.LittleEndian.PutUint16(data[pos:], uint16(len(key)))
		pos += 2
		copy(data[pos:], key)
		pos += len(key)
	}

	if node.IsLeaf {
		// 写入值
		for _, val := range node.Values {
			if err := ensure(2 + len(val)); err != nil {
				return err
			}
			binary.LittleEndian.PutUint16(data[pos:], uint16(len(val)))
			pos += 2
			copy(data[pos:], val)
			pos += len(val)
		}
	} else {
		// 写入子节点指针
		for _, child := range node.Children {
			if err := ensure(4); err != nil {
				return err
			}
			binary.LittleEndian.PutUint32(data[pos:], uint32(child))
			pos += 4
		}
	}

	page, err := t.pager.ReadPage(node.PageId)
	if err != nil {
		return err
	}

	if err := page.SetData(data); err != nil {
		return err
	}

	t.pager.MarkDirty(node.PageId)
	return nil
}

// Verify 校验 B+Tree 完整性
func (t *BTree) Verify() error {
	// 校验树结构
	if err := t.verifyNode(t.rootPage, nil, nil, 0); err != nil {
		return fmt.Errorf("tree structure error: %w", err)
	}

	// 校验叶子链表
	if err := t.verifyLeafChain(); err != nil {
		return fmt.Errorf("leaf chain error: %w", err)
	}

	return nil
}

// CheckTreeIntegrity 校验 B+Tree 完整性（公开别名）
func (t *BTree) CheckTreeIntegrity() error {
	return t.Verify()
}

// verifyNode 递归校验节点
func (t *BTree) verifyNode(pageId PageId, minKey, maxKey []byte, depth int) error {
	node, err := t.readNode(pageId)
	if err != nil {
		return fmt.Errorf("failed to read node %d: %w", pageId, err)
	}

	// 校验键数量（根节点例外）
	if pageId != t.rootPage {
		if node.KeyCount < MinKeys() {
			return fmt.Errorf("node %d has too few keys: %d < %d", pageId, node.KeyCount, MinKeys())
		}
	}
	if node.KeyCount > BTreeOrder-1 {
		return fmt.Errorf("node %d has too many keys: %d > %d", pageId, node.KeyCount, BTreeOrder-1)
	}

	// 校验键的顺序
	for i := 1; i < node.KeyCount; i++ {
		if bytes.Compare(node.Keys[i-1], node.Keys[i]) >= 0 {
			return fmt.Errorf("node %d keys not in order at index %d", pageId, i)
		}
	}

	// 校验键在范围内
	if minKey != nil && node.KeyCount > 0 {
		if bytes.Compare(node.Keys[0], minKey) < 0 {
			return fmt.Errorf("node %d first key less than min bound", pageId)
		}
	}
	if maxKey != nil && node.KeyCount > 0 {
		if bytes.Compare(node.Keys[node.KeyCount-1], maxKey) >= 0 {
			return fmt.Errorf("node %d last key exceeds max bound", pageId)
		}
	}

	// 递归校验子节点
	if !node.IsLeaf {
		if len(node.Children) != node.KeyCount+1 {
			return fmt.Errorf("node %d has incorrect child count: %d vs %d", pageId, len(node.Children), node.KeyCount+1)
		}

		for i, childId := range node.Children {
			var childMin, childMax []byte
			if i > 0 {
				childMin = node.Keys[i-1]
			} else {
				childMin = minKey
			}
			if i < node.KeyCount {
				childMax = node.Keys[i]
			} else {
				childMax = maxKey
			}

			if err := t.verifyNode(childId, childMin, childMax, depth+1); err != nil {
				return err
			}
		}
	} else {
		// 叶子节点：校验值数量
		if len(node.Values) != node.KeyCount {
			return fmt.Errorf("leaf node %d has mismatched values count: %d vs %d", pageId, len(node.Values), node.KeyCount)
		}
	}

	return nil
}

// CheckLeafChain 校验叶子节点链表（公开方法）
func (t *BTree) CheckLeafChain() error {
	return t.verifyLeafChain()
}

// verifyLeafChain 校验叶子节点链表
func (t *BTree) verifyLeafChain() error {
	// 找到最左叶子节点
	node, err := t.readNode(t.rootPage)
	if err != nil {
		return err
	}

	for !node.IsLeaf {
		if len(node.Children) == 0 {
			return fmt.Errorf("internal node %d has no children", node.PageId)
		}
		node, err = t.readNode(node.Children[0])
		if err != nil {
			return err
		}
	}

	// 遍历叶子链表，验证双向链接
	var prevNode *BTreeNode
	var lastKey []byte
	count := 0

	for node != nil {
		count++
		if count > 1000000 { // 防止无限循环
			return fmt.Errorf("leaf chain too long, possible cycle")
		}

		// 验证 Prev 指针
		if prevNode != nil {
			if node.Prev != prevNode.PageId {
				return fmt.Errorf("leaf node %d has incorrect Prev pointer: %d vs %d", node.PageId, node.Prev, prevNode.PageId)
			}
		} else {
			if node.Prev != 0 {
				return fmt.Errorf("first leaf node %d has non-zero Prev: %d", node.PageId, node.Prev)
			}
		}

		// 验证键顺序跨节点
		if lastKey != nil && node.KeyCount > 0 {
			if bytes.Compare(lastKey, node.Keys[0]) >= 0 {
				return fmt.Errorf("leaf chain order violation at node %d", node.PageId)
			}
		}

		if node.KeyCount > 0 {
			lastKey = node.Keys[node.KeyCount-1]
		}

		// 移动到下一个节点
		prevNode = node
		if node.Next == 0 {
			break
		}
		node, err = t.readNode(node.Next)
		if err != nil {
			return err
		}
	}

	return nil
}

// Count 返回 B+Tree 中的键总数
func (t *BTree) Count() (int, error) {
	count := 0

	// 找到最左叶子节点
	node, err := t.readNode(t.rootPage)
	if err != nil {
		return 0, err
	}

	for !node.IsLeaf {
		if len(node.Children) == 0 {
			break
		}
		node, err = t.readNode(node.Children[0])
		if err != nil {
			return 0, err
		}
	}

	// 遍历叶子链表计数
	for node != nil {
		count += node.KeyCount
		if node.Next == 0 {
			break
		}
		node, err = t.readNode(node.Next)
		if err != nil {
			return 0, err
		}
	}

	return count, nil
}

// GetAllKeys 获取所有键（按顺序）
func (t *BTree) GetAllKeys() ([][]byte, error) {
	keys := make([][]byte, 0)

	// 找到最左叶子节点
	node, err := t.readNode(t.rootPage)
	if err != nil {
		return nil, err
	}

	for !node.IsLeaf {
		if len(node.Children) == 0 {
			break
		}
		node, err = t.readNode(node.Children[0])
		if err != nil {
			return nil, err
		}
	}

	// 遍历叶子链表收集键
	for node != nil {
		keys = append(keys, node.Keys...)
		if node.Next == 0 {
			break
		}
		node, err = t.readNode(node.Next)
		if err != nil {
			return nil, err
		}
	}

	return keys, nil
}

// Height 返回 B+Tree 的高度
func (t *BTree) Height() (int, error) {
	height := 0
	node, err := t.readNode(t.rootPage)
	if err != nil {
		return 0, err
	}

	for {
		height++
		if node.IsLeaf {
			break
		}
		if len(node.Children) == 0 {
			break
		}
		node, err = t.readNode(node.Children[0])
		if err != nil {
			return 0, err
		}
	}

	return height, nil
}

// 辅助函数

func insertAt(slice [][]byte, index int, value []byte) [][]byte {
	slice = append(slice, nil)
	copy(slice[index+1:], slice[index:])
	slice[index] = value
	return slice
}

func insertPageIdAt(slice []PageId, index int, value PageId) []PageId {
	slice = append(slice, 0)
	copy(slice[index+1:], slice[index:])
	slice[index] = value
	return slice
}
