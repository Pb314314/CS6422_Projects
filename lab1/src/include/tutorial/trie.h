/*******************************************************
 * File: trie.cpp
 * Author: Bo Pang
 * GT ID: 903924447
 * Email: bpang42@gatech.edu
 * 
 * Description:
 * This file contains the implementation of CS6422 Assignment1;
 * Header file of trie.
 *******************************************************/

#pragma once

#include <iostream>
#include <vector>

using namespace std;
namespace buzzdb {
namespace tutorial {
class Node{
public:
    Node();
    vector<Node*> letter_vec;//用来指向26个字母。
    //每一个Node对应了一个word
    vector<int> location;//用来记录这个word的index-location关系
    bool end;
};

class Trie {
public:
    /** Initialize your data structure here. */
    Node* root;
    Trie();
    
    ~Trie();
    void ReleaseMemory(Node* &ptr);

    /** Inserts a word into the trie. */
    void insert(string word);
    
    /** Returns if the word is in the trie. */
    bool  search(string word);
    
    /** Returns if there is any word in the trie that starts with the given prefix. */
    bool startsWith(string prefix);

    void insert_with_loc(string word, int index);

    void search_with_loc(string word, int index,string &result);
};
}
}
