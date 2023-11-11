/*******************************************************
 * File: trie.cpp
 * Author: Bo Pang
 * GT ID: 903924447
 * Email: bpang42@gatech.edu
 * 
 * Description:
 * This file contains the implementation of CS6422 Assignment1;
 * Impliment the Trie structure;
 *******************************************************/

#include <iostream>
#include <vector>
#include "tutorial/trie.h"


using namespace std;
namespace buzzdb {
namespace tutorial {

// Constructor for Node;
Node::Node(): letter_vec(37,NULL), location({}),end(false) {}
    
// Constructor for Trie;
Trie::Trie() {root = new Node;}

// Use new in the constructor, so need destructor;
Trie::~Trie() {ReleaseMemory(root);}

// Release the memory for Trie;
// Input: ptr: pointer to the memory to deallocate;
void Trie::ReleaseMemory(Node* &ptr){//use reference to set the deleted pointer to nullptr!!;
    if(ptr == NULL) return;
    //cout<< "ReleaseMemory" <<endl;
    for(Node* i : ptr->letter_vec){
        if(i) ReleaseMemory(i);
    }
    delete(ptr);
    ptr = nullptr;
    return;
}

// Basic inserts a word into the trie;
// Input: word: a string of word;
void Trie::insert(string word) {
    Node* ptr = root;
    for(char c:word){ 
        int ind = c - 'a';
        if(ptr->letter_vec[ind] == NULL){
            ptr->letter_vec[ind] = new Node;
        }
       ptr = ptr->letter_vec[ind];
    }
    ptr->end = true;
    return;
}
    
// Basic search in Trie;
// Input: word: a string of word;
bool Trie::search(string word) {
    Node* ptr = root;
    for(char c : word){
        int ind = c - 'a';
        if(!ptr->letter_vec[ind]){
            return false;
        }
        ptr = ptr->letter_vec[ind];
    }
    if(ptr->end) return true;
    return false;
}
    
// Returns if there is any word in the trie that starts with the given prefix;
// Input: prefix: a string of word;
bool Trie::startsWith(string prefix) {
    Node* ptr = root;
    for(char c: prefix){
        int ind = c - 'a';
        if(!ptr->letter_vec[ind]){
            return false;
        }
        ptr = ptr->letter_vec[ind];
    }
    return true;   
}

// Insert with other valid letter;
// Input: word: a string of word. index: the index of the word in the file;
void Trie::insert_with_loc(string word, int index){
    Node* ptr = root;
    int ind;
    for(char c:word){ 
        if(c>='a' && c<= 'z') ind = c - 'a';
        else if(c>='0' && c <='9') ind = 26 + (c - '0');
        else ind = 36;
        if(ptr->letter_vec[ind] == NULL){
            ptr->letter_vec[ind] = new Node;
        }
       ptr = ptr->letter_vec[ind];
    }
    ptr->end = true;
    ptr->location.push_back(index);
    return;
}

// Search the location of the word in the Trie;
// Input: word: a string of word. index: the appearance time of the word to search, result: store the result;
void Trie::search_with_loc(string word, int index, string &result){
    Node* ptr = root;
    if(ptr == NULL){
        // no file loaded
        result = "No matching entry";
        return;
    }
    int ind;
    for(char c : word){
        if(c>='a'&& c<='z') ind = c - 'a';
        else if(c>='0'&& c<='1') ind = c - '0';
        else ind = 36;
        if(ind < 0 || ind >= 37){
            // out of index
            result = "ERROR: Invalid command";
            return;
        }
        if(ptr->letter_vec[ind] == NULL){
            result = "No matching entry";
            return;
        }
        ptr = ptr->letter_vec[ind];
    }
    if(ptr->end){
        if((int)ptr->location.size() >= index){// avoud sig fault;
            result = to_string(ptr->location[index-1]);
            return;
        }
    } 
    result = "No matching entry";
    return;
}   

}
}