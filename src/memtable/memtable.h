#ifndef MEMTABLE_H
#define MEMTABLE_H

#include <string>
#include <iostream>
#include <algorithm>

enum Color { RED, BLACK };

template<typename K, typename V>
class RedBlackNode {
public:
    K key;
    V value;
    Color color;
    RedBlackNode* left;
    RedBlackNode* right;
    RedBlackNode* parent;

    RedBlackNode(const K& k, const V& v, Color c = RED)
        : key(k), value(v), color(c), left(nullptr), right(nullptr), parent(nullptr) {}
};

template<typename K, typename V>
class RedBlackTree {
private:
    RedBlackNode<K, V>* root;
    RedBlackNode<K, V>* nil_node;
    size_t current_size;
    size_t memtable_size;

    void left_rotate(RedBlackNode<K, V>* x);
    void right_rotate(RedBlackNode<K, V>* y);
    void insert_fixup(RedBlackNode<K, V>* z);
    void transplant(RedBlackNode<K, V>* u, RedBlackNode<K, V>* v);
    RedBlackNode<K, V>* minimum(RedBlackNode<K, V>* x);
    void delete_fixup(RedBlackNode<K, V>* x);
    void destroy_tree(RedBlackNode<K, V>* node);
    bool verify_red_black_helper(RedBlackNode<K, V>* node) const;
    int get_height_helper(RedBlackNode<K, V>* node) const;

public:
    RedBlackTree(size_t max_size);
    ~RedBlackTree();

    bool put(const K& key, const V& value);
    bool get(const K& key, V& value);
    bool is_full() const;
    size_t size() const;
    void clear();

    void inorder_traversal() const;
    void inorder_helper(RedBlackNode<K, V>* node) const;

    // TODO: scan method for range queries
    // std::vector<std::pair<K, V>> scan(const K& start_key, const K& end_key) const;
    // void scan_helper(RedBlackNode<K, V>* node, const K& start_key, const K& end_key, std::vector<std::pair<K, V>>& results) const;

    RedBlackNode<K, V>* get_root() const { return root; }
    RedBlackNode<K, V>* get_nil_node() const { return nil_node; }
    int get_black_height(RedBlackNode<K, V>* node) const;
    bool verify_red_black_properties() const;
    int get_tree_height() const;
};

#include "memtable.cpp"

#endif
