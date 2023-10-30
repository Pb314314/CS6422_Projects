/* 
*This is a header comment for the B+ Tree implementation.
* Author: Bopang
* Description: This code represents the B+ Tree data structure.
* Date: 10/28/2023
*/

#pragma once

#include <cstddef>
#include <cstring>
#include <functional>
#include <iostream>
#include <optional>

#include "buffer/buffer_manager.h"
#include "common/defer.h"
#include "common/macros.h"
#include "storage/segment.h"

#define UNUSED(p)  ((void)(p))
using namespace std;
namespace buzzdb {

template<typename KeyT, typename ValueT, typename ComparatorT, size_t PageSize>

struct BTree : public Segment {
    struct Node {

        /// The level in the tree.
        uint16_t level;

        /// The number of children.
        uint16_t count;

        // Constructor
        Node(uint16_t level, uint16_t count)
            : level(level), count(count) {}

        /// Is the node a leaf node?
        bool is_leaf() const { return level == 0; }
    };

    struct InnerNode: public Node {
        /// The capacity of a node.
        static constexpr uint32_t kCapacity = 42;

        /// The keys.
        KeyT keys[kCapacity];

        /// The children.
        uint64_t children[kCapacity];

        /// Constructor.
        InnerNode() : Node(1, 0) {}

        /// Get the index of the first key that is not less than than a provided key.
        /// @param[in] key          The key that should be searched.
        uint32_t lower_bound(const KeyT &key) {
            // count是children的个数 key的个数是count-1
            // left: less than key. right：>= key
            if(this->count == 2){
                if(key < keys[0]){
                    return 0;
                }
                else return 1;
            }
            uint32_t l = -1;
            uint32_t r = this->count-1;// key的个数是count-1
            while((l+1) != r){
                uint32_t mid = (l+r)/2;
                if(keys[mid] < key){
                    l = mid;
                }
                else r = mid;
            }
            // 此时r指向keys中第一个大于等于key的key
            return r;
        }

        /// Insert a key.
        /// @param[in] key          The separator that should be inserted.
        /// @param[in] split_page   The id of the split page that should be inserted.
        void insert(const KeyT &key, uint64_t split_page) {
	        // 通过lower bound,找到key应该insert的位置
            uint32_t index = lower_bound(key);
            for(uint32_t i=this->count-1;i>index;i--){
                keys[i] = keys[i-1];
                children[i+1] = children[i-1+1];
            } 
            keys[index] = key;
            children[index+1] = split_page;
            this->count++;
            return;
        }

        /// Split the node.
        /// @param[in] buffer       The buffer for the new page.
        /// @return                 The separator key.
        KeyT split(std::byte* buffer) {
            InnerNode* split_node = new (buffer) InnerNode();
            // Move last half of key-values to the new node
            uint32_t index = (kCapacity-1)/2;
            
            uint32_t key_num = 0;
            // 将index指向的值给上一层 然后左右分成两个node
            for(uint16_t i= index+1;i < (this->count-1);i++){
                split_node->keys[key_num] = keys[i];
                split_node->children[key_num] = children[i];
                key_num++;
            }
            split_node->children[key_num] = children[this->count-1];
            key_num++; // children 比 keys多一个 所以要多copy一个

            split_node->count = key_num;
            split_node->level = 1;
            this->count -=  key_num;
            return keys[index]; 
        }

        /// Returns the keys.
        /// Can be implemented inefficiently as it's only used in the tests.
        std::vector<KeyT> get_key_vector() {
            // TODO
        }

        /// Returns the child page ids.
        /// Can be implemented inefficiently as it's only used in the tests.
        std::vector<uint64_t> get_child_vector() {
            // TODO
	    return std::vector<uint64_t>();
        }
    };
    
    /// Return type of recursive function. The split information.
    struct ifsplit{
        bool split;
        KeyT key;
        uint64_t page_id;
    };

    struct LeafNode: public Node {
        /// The capacity of a node.
        static constexpr uint32_t kCapacity = 42;

        /// The keys.
        KeyT keys[kCapacity];

        /// The values.
        ValueT values[kCapacity];

        /// Constructor.
        LeafNode() : Node(0, 0) {}   // Node(level, count)

        /// Insert a key.
        /// @param[in] key          The key that should be inserted.
        /// @param[in] value        The value that should be inserted.
        void insert(const KeyT &key, const ValueT &value) {
            // 这个函数是找到leaf node之后在leaf node中插入key value
            // have space left, just insert.
            // if empty
            if(this->count == 0){
                keys[this->count] = key;
                values[this->count] = value;
                this->count++;
                return;
            }
            // not empty, use binary search, find the first key bigger than current key
            int l = -1;
            int r = this->count;
            //左边： 小于k    右边： 大于等于k
            while((l+1) != r){
                int mid = (l+r)/2;
                if(keys[mid] < key){ // 
                    l = mid; 
                }
                else r = mid;
            }
            // find the location:r to insert
            if(keys[r] == key){
                values[r] = value;
            }
            else{
            for(int i=this->count;i>r;i--){
                keys[i] = keys[i-1];
                values[i] = values[i-1];
            }
            keys[r] = key;
            values[r] = value;
            this->count++;
            }
            return;
        }

        /// Erase a key.
        void erase(const KeyT &key) {
            if(this->count == 0) return;
            int l = -1;
            int r = this->count;
            //左边： 小于k    右边： 大于等于k
            while((l+1) != r){
                int mid = (l+r)/2;
                if(keys[mid] < key){ // 
                    l = mid; 
                }
                else r = mid;
            }
            if(keys[r] == key){
                for(int i= r;i<(this->count-1);i++){
                    keys[i] = keys[i+1];
                    values[i] = keys[i+1];
                }
                this->count--;
                return;
            }
            else return;
        }

        /// Split the node.
        /// @param[in] buffer       The buffer for the new page.
        /// @return                 The separator key.
        KeyT split(std::byte* buffer) {
            LeafNode* split_node = new (buffer) LeafNode();
            // move last half of key-values to the new node
            int key_num = 0;
            for(uint32_t i= kCapacity/2;i<kCapacity;i++){
                split_node->keys[key_num] = keys[i];
                split_node->values[key_num] = values[i];
                key_num++;
            }
            split_node->count = key_num;
            this->count -=  key_num;
            return split_node->keys[0]; // 将右侧新split node的第一个key给到上层inner node
        }

        /// Returns the keys.
        /// Can be implemented inefficiently as it's only used in the tests.
        std::vector<KeyT> get_key_vector() {
            // TODO
        }

        /// Returns the values.
        /// Can be implemented inefficiently as it's only used in the tests.
        std::vector<ValueT> get_value_vector() {
            // TODO
        }
    };

    /// The root.
    std::optional<uint64_t> root;

    /// Next page id.
    /// You don't need to worry about about the page allocation.
    /// Just increment the next_page_id whenever you need a new page.
    uint64_t next_page_id;

    /// Constructor.
    BTree(uint16_t segment_id, BufferManager &buffer_manager)
        : Segment(segment_id, buffer_manager) {
        // TODO
        next_page_id = 1;
    }

    /// recursive function for lookup. Start from root node and recursively find the leaf node of given key.
    std::optional<ValueT> recur_lookup(const KeyT &key, uint64_t page_id){
        BufferFrame &current_frame = buffer_manager.fix_page(page_id , true);
        Node* current_node = reinterpret_cast<BTree::Node*>(current_frame.get_data());
        if(current_node->is_leaf()){    // already find the leaf node of key.                     
            LeafNode* leaf_node = static_cast<BTree::LeafNode*>(current_node);
            if(leaf_node->count == 0) return {};
            // 左边： 小于  右边： 大于等于
            int32_t l = -1;
            int32_t r = leaf_node->count;
            while((l+1) != r){
                int32_t mid = (l+r)/2;  //找到第一个大于的元素
                if(leaf_node->keys[mid] < key)  l = mid;
                else r = mid;
            }
            if(leaf_node->keys[r] == key) return leaf_node->values[r];
            else return {}; // can't find the given key
        }
        // Search and find child
        InnerNode* inner_node = static_cast<BTree::InnerNode*>(current_node);
        // 左边： 小于等于  右边： 大于（r指向对应的children的index）
        int32_t l = -1;
        int32_t r = inner_node->count-1;
        while((l+1) != r){
            int32_t mid = (l+r)/2;
            if(inner_node->keys[mid] <= key)  l = mid;//小于key
            else r = mid;// 大于key
        }
        return recur_lookup(key, inner_node->children[r]);
    }

    /// Lookup an entry in the tree.
    /// @param[in] key      The key that should be searched.
    std::optional<ValueT> lookup(const KeyT &key) {
        if (!root.has_value()) return {};
        return recur_lookup(key, root.value());
    }

    // recursive function for erase. Start from root node and recursively find the leaf node of given key and delete key_value.
    void erase_recur(const KeyT &key ,uint64_t page_id){
        BufferFrame &current_frame = buffer_manager.fix_page(page_id , true);
        Node* current_node = reinterpret_cast<BTree::Node*>(current_frame.get_data());
        if(current_node->is_leaf()){
            // cast to leafnode
            LeafNode* leaf_node = static_cast<BTree::LeafNode*>(current_node);
            return leaf_node->erase(key);
        }
        // Current is innernode:
        InnerNode* inner_node = static_cast<BTree::InnerNode*>(current_node);
        // 左边： 小于等于  右边： 大于（r指向对应的children的index）
        int32_t l = -1;
        int32_t r = inner_node->count-1;
        while((l+1) != r){
            int32_t mid = (l+r)/2;
            if(inner_node->keys[mid] <= key)  l = mid;//小于key
            else r = mid;// 大于等于key
        }
        erase_recur(key, inner_node->children[r]);
        return;
    }

    /// Erase an entry in the tree.
    /// @param[in] key      The key that should be searched.
    void erase(const KeyT &key) {
        if (!root.has_value()) {
	      // the tree is empty
	      return;
	    }
        erase_recur(key, root.value());
    }
    /// recursive function for insert. Start from root node and recursively find the leaf node to insert key_value. 
    /// Call leaf_node->insert(key_value); 
    /// If split, can return back from leaf_node call inner_node->insert(key_children);
    ifsplit insert_recur(const KeyT &key, const ValueT &value,uint64_t page_id){
        BufferFrame &current_frame = buffer_manager.fix_page(page_id , true);
        Node* current_node = reinterpret_cast<BTree::Node*>(current_frame.get_data());
        if(current_node->is_leaf()){
            LeafNode* leaf_node = static_cast<BTree::LeafNode*>(current_node);
            return leafnode_insert(key,value,leaf_node);
        }
        // Current is innernode:
        InnerNode* inner_node = static_cast<BTree::InnerNode*>(current_node);
        int32_t l = -1;
        int32_t r = inner_node->count-1;
        while((l+1) != r){
            int32_t mid = (l+r)/2;
            if(inner_node->keys[mid] <= key)  l = mid;//小于key
            else r = mid;// 大于key
        }
        ifsplit split_info = insert_recur(key, value, inner_node->children[r]);
        if(split_info.split){
            // need insert split into inner node
            // Insert key-children into inner node!
            return innernode_insert(split_info.key , split_info.page_id, inner_node);
        }
        else{
            ifsplit result = {false,0,0};// split key page_id
            return result;
        }
    }

    /// General insert into a inner_node
    ifsplit innernode_insert(const KeyT &key,uint64_t child_page_id, InnerNode* inner_node){ 
        //root is innernode
        if(inner_node->count < inner_node->kCapacity){
            inner_node->insert(key, child_page_id);
            ifsplit result = {false,0,0};
            return result;
        }
        // need split;
        // create a new inner node
        uint64_t split_page_id = buffer_manager.get_overall_page_id(segment_id, next_page_id);
        BufferFrame &split_frame = buffer_manager.fix_page(split_page_id,true);
        next_page_id++;
        // copy key-values to the new node
        KeyT split_value =  inner_node->split((std::byte *)(split_frame.get_data()));
        // Insert key_value to inner node
        if(key < split_value){
            inner_node->insert(key, child_page_id);
        }
        else{
            InnerNode* split_node = reinterpret_cast<BTree::InnerNode*>(split_frame.get_data());
            split_node->insert(key, child_page_id);
        }
        buffer_manager.unfix_page(split_frame, true);
        ifsplit result = {true,split_value,split_page_id};
        return result;
    }

    // General insert into leafnode.
    ifsplit leafnode_insert(const KeyT &key, const ValueT &value,LeafNode* leaf_node){
        if(leaf_node->count < leaf_node->kCapacity){
            leaf_node->insert(key, value);
            ifsplit result = {false,0,0};
            return result;
        }
        // need split;
        // create a new leaf node
        uint64_t split_page_id = buffer_manager.get_overall_page_id(segment_id, next_page_id);
        BufferFrame &split_frame = buffer_manager.fix_page(split_page_id,true);
        next_page_id++;
        // copy key-values to the new node
        KeyT split_value =  leaf_node->split((std::byte *)(split_frame.get_data()));
        // Insert key_value to leaf node
        if(key < split_value){
            leaf_node->insert(key, value);
        }
        else{
            LeafNode* split_node = reinterpret_cast<BTree::LeafNode*>(split_frame.get_data());
            split_node->insert(key, value);
        }
        buffer_manager.unfix_page(split_frame, true);
        ifsplit result = {true,split_value,split_page_id};
        return result;
    }

    /// Inserts a new entry into the tree.
    /// @param[in] key      The key that should be inserted.
    /// @param[in] value    The value that should be inserted.
    void insert(const KeyT &key, const ValueT &value) {
	    // if tree is empty, create a new root node;
	    if (!root.has_value()) {
	      BufferFrame &frame = buffer_manager.fix_page(//用buffer_manager建立了一个frame(一个page) 
		  buffer_manager.get_overall_page_id(segment_id, next_page_id), true);
          // replacement in new. 给new指定new的位置。
	      auto *root_node = new (frame.get_data()) LeafNode(); // 用这个buffer_frame的空间生成node
	      this->root = buffer_manager.get_overall_page_id(segment_id, next_page_id); // root is the page_id of the node
          next_page_id++;
          root_node->insert(key, value); // dont need the return value to check split or not;
	      buffer_manager.unfix_page(frame, true);
	      return;
	    }
        // Have root node, insert into tree.
        ifsplit result = insert_recur(key,value, root.value());
        if(result.split){
            // create a new root node;
            uint64_t new_root_page_id= buffer_manager.get_overall_page_id(segment_id, next_page_id);
            // root already split into two nodes, need a new root;
            BufferFrame &split_root_frame = buffer_manager.fix_page(new_root_page_id,true);
          // replacement in new. 给new指定new的位置。
            auto *root_node = new (split_root_frame.get_data()) InnerNode(); // 用这个buffer_frame的空间生成node
            root_node->keys[0] = result.key;
            root_node->children[0] = root.value();
            root_node->children[1] = result.page_id;
            root_node->count = 2;
            root_node->level = 1;
            this->root = new_root_page_id; // root is the page_id of the node
            next_page_id++;
        }
        //cout<< "*******************************************************************"<<endl;
        //node_info(root.value());
        return;
    }

    // just for test to see the tree structure
    void node_info(uint64_t page_id){
        BufferFrame &current_frame = buffer_manager.fix_page(page_id,true);
        Node* current_node = reinterpret_cast<BTree::Node*>(current_frame.get_data());
        if(current_node->is_leaf()){
            LeafNode* leaf_node = static_cast<BTree::LeafNode*>(current_node);
            cout<< "Leaf node:  " << page_id <<endl;
            cout<< "Keys:";
            for(uint16_t i=0;i<leaf_node->count;i++){
                cout<< leaf_node->keys[i]<<"  ";
            }
            cout<<endl;
            cout<< "Values:";
            for(uint16_t i=0;i<leaf_node->count;i++){
                cout<< leaf_node->values[i]<<"  ";
            }
            cout<<endl;
            cout<<endl;
            return;
        }
        InnerNode* inner_node = static_cast<BTree::InnerNode*>(current_node);
        cout<< "Inner node  :" << page_id <<endl;
            cout<< "Keys:";
            for(uint16_t i=0;i<inner_node->count-1;i++){
                cout<< inner_node->keys[i]<<"  ";
            }
            cout<<endl;
            cout<< "Children:";
            for(uint16_t i=0;i<inner_node->count;i++){
                cout<< inner_node->children[i]<<"  ";
            }
            cout<<endl;
            cout<<endl;
            for(uint16_t i=0;i<inner_node->count;i++){
                node_info(inner_node->children[i]);
            }
            return;
    }
};

}  // namespace buzzdb
