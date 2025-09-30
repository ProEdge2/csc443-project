#ifndef MEMTABLE_CPP
#define MEMTABLE_CPP

#include "memtable.h"

template<typename K, typename V>
RedBlackTree<K, V>::RedBlackTree(size_t max_size) : current_size(0), memtable_size(max_size) {
    nil_node = new RedBlackNode<K, V>(K{}, V{}, BLACK);
    nil_node->left = nil_node;
    nil_node->right = nil_node;
    nil_node->parent = nil_node;
    root = nil_node;
}

template<typename K, typename V>
RedBlackTree<K, V>::~RedBlackTree() {
    destroy_tree(root);
    delete nil_node;
}

template<typename K, typename V>
void RedBlackTree<K, V>::destroy_tree(RedBlackNode<K, V>* node) {
    if (node != nil_node) {
        destroy_tree(node->left);
        destroy_tree(node->right);
        delete node;
    }
}

template<typename K, typename V>
void RedBlackTree<K, V>::left_rotate(RedBlackNode<K, V>* x) {
    RedBlackNode<K, V>* y = x->right;
    x->right = y->left;

    if (y->left != nil_node) {
        y->left->parent = x;
    }

    y->parent = x->parent;

    if (x->parent == nil_node) {
        root = y;
    } else if (x == x->parent->left) {
        x->parent->left = y;
    } else {
        x->parent->right = y;
    }

    y->left = x;
    x->parent = y;
}

template<typename K, typename V>
void RedBlackTree<K, V>::right_rotate(RedBlackNode<K, V>* y) {
    RedBlackNode<K, V>* x = y->left;
    y->left = x->right;

    if (x->right != nil_node) {
        x->right->parent = y;
    }

    x->parent = y->parent;

    if (y->parent == nil_node) {
        root = x;
    } else if (y == y->parent->right) {
        y->parent->right = x;
    } else {
        y->parent->left = x;
    }

    x->right = y;
    y->parent = x;
}

template<typename K, typename V>
bool RedBlackTree<K, V>::put(const K& key, const V& value) {
    RedBlackNode<K, V>* z = new RedBlackNode<K, V>(key, value);
    RedBlackNode<K, V>* y = nil_node;
    RedBlackNode<K, V>* x = root;

    while (x != nil_node) {
        y = x;

        if (z->key == x->key) {
            x->value = value;
            delete z;
            return true;
        } else if (z->key < x->key) {
            x = x->left;
        } else {
            x = x->right;
        }
    }

    if (current_size >= memtable_size) {
        delete z;
        return false;
    }

    z->parent = y;
    if (y == nil_node) {
        root = z;
    } else if (z->key < y->key) {
        y->left = z;
    } else {
        y->right = z;
    }

    z->left = nil_node;
    z->right = nil_node;
    z->color = RED;

    insert_fixup(z);
    current_size++;
    return true;
}

template<typename K, typename V>
void RedBlackTree<K, V>::insert_fixup(RedBlackNode<K, V>* z) {
    while (z->parent->color == RED) {
        if (z->parent == z->parent->parent->left) {
            RedBlackNode<K, V>* y = z->parent->parent->right;

            if (y->color == RED) {
                z->parent->color = BLACK;
                y->color = BLACK;
                z->parent->parent->color = RED;
                z = z->parent->parent;

            } else {
                if (z == z->parent->right) {
                    z = z->parent;
                    left_rotate(z);
                }
                z->parent->color = BLACK;
                z->parent->parent->color = RED;
                right_rotate(z->parent->parent);
            }

        } else {
            RedBlackNode<K, V>* y = z->parent->parent->left;


            if (y->color == RED) {
                z->parent->color = BLACK;
                y->color = BLACK;
                z->parent->parent->color = RED;
                z = z->parent->parent;
            } else {
                if (z == z->parent->left) {
                    z = z->parent;
                    right_rotate(z);
                }

                z->parent->color = BLACK;
                z->parent->parent->color = RED;
                left_rotate(z->parent->parent);
            }
        }
    }

    root->color = BLACK;
}

template<typename K, typename V>
bool RedBlackTree<K, V>::get(const K& key, V& value) {
    RedBlackNode<K, V>* current = root;

    while (current != nil_node) {
        if (key == current->key) {
            value = current->value;

            return true;
        } else if (key < current->key) {
            current = current->left;
        } else {
            current = current->right;
        }
    }

    return false;
}

template<typename K, typename V>
bool RedBlackTree<K, V>::is_full() const {
    return current_size >= memtable_size;
}

template<typename K, typename V>
size_t RedBlackTree<K, V>::size() const {
    return current_size;
}

template<typename K, typename V>
void RedBlackTree<K, V>::clear() {
    destroy_tree(root);
    root = nil_node;
    current_size = 0;
}

template<typename K, typename V>
void RedBlackTree<K, V>::inorder_traversal() const {
    inorder_helper(root);
    std::cout << std::endl;
}

template<typename K, typename V>
void RedBlackTree<K, V>::inorder_helper(RedBlackNode<K, V>* node) const {
    if (node != nil_node) {
        inorder_helper(node->left);
        std::cout << "(" << node->key << ", " << node->value << ") ";
        inorder_helper(node->right);
    }
}

template<typename K, typename V>
int RedBlackTree<K, V>::get_black_height(RedBlackNode<K, V>* node) const {
    if (node == nil_node) {
        return 1;
    }

    int left_height = get_black_height(node->left);
    if (left_height == -1) return -1;

    int right_height = get_black_height(node->right);
    if (right_height == -1) return -1;

    if (left_height != right_height) {
        return -1;
    }

    return left_height + (node->color == BLACK ? 1 : 0);
}

template<typename K, typename V>
bool RedBlackTree<K, V>::verify_red_black_properties() const {
    if (root == nil_node) return true;

    if (root->color != BLACK) return false;

    return verify_red_black_helper(root);
}

template<typename K, typename V>
bool RedBlackTree<K, V>::verify_red_black_helper(RedBlackNode<K, V>* node) const {
    if (node == nil_node) return true;

    if (node->color == RED) {
        if (node->left->color == RED || node->right->color == RED) {
            return false;
        }
    }

    return verify_red_black_helper(node->left) && verify_red_black_helper(node->right);
}

template<typename K, typename V>
int RedBlackTree<K, V>::get_tree_height() const {
    return get_height_helper(root);
}

template<typename K, typename V>
int RedBlackTree<K, V>::get_height_helper(RedBlackNode<K, V>* node) const {
    if (node == nil_node) return 0;

    int left_height = get_height_helper(node->left);
    int right_height = get_height_helper(node->right);

    return 1 + std::max(left_height, right_height);
}

template<typename K, typename V>
std::vector<std::pair<K, V>> RedBlackTree<K, V>::scan(const K& start_key, const K& end_key) const {
    std::vector<std::pair<K, V>> results;
    scan_helper(root, start_key, end_key, results);
    return results;
}

template<typename K, typename V>
void RedBlackTree<K, V>::scan_helper(RedBlackNode<K, V>* node, const K& start_key, const K& end_key, std::vector<std::pair<K, V>>& results) const {
    if (node == nil_node) {
        return;
    }

    // If current node's key is greater than start_key, search left subtree
    if (start_key < node->key) {
        scan_helper(node->left, start_key, end_key, results);
    }

    // If current node's key is within the range, add it to results
    if (node->key >= start_key && node->key <= end_key) {
        results.push_back(std::make_pair(node->key, node->value));
    }

    // If current node's key is less than end_key, search right subtree
    if (end_key > node->key) {
        scan_helper(node->right, start_key, end_key, results);
    }
}

template<typename K, typename V>
K RedBlackTree<K, V>::get_min_key() const {
    if (root == nil_node) {
        return K{}; // Return default value for empty tree
    }

    RedBlackNode<K, V>* current = root;
    while (current->left != nil_node) {
        current = current->left;
    }
    return current->key;
}

template<typename K, typename V>
K RedBlackTree<K, V>::get_max_key() const {
    if (root == nil_node) {
        return K{}; // Return default value for empty tree
    }

    RedBlackNode<K, V>* current = root;
    while (current->right != nil_node) {
        current = current->right;
    }
    return current->key;
}

#endif
