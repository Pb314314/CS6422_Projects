/*******************************************************
 * File: trie.cpp
 * Author: Bo Pang
 * GT ID: 903924447
 * Email: bpang42@gatech.edu
 * 
 * Description:
 * This file contains the implementation of CS6422 Assignment1;
 * Header file of tutorial.
 *******************************************************/


#pragma once    // Avoid multiple times of include header file.

#include <string>
#include "tutorial/trie.h"

using namespace std;

namespace buzzdb {
namespace tutorial {

class CommandExecutor {
 public:
  CommandExecutor();
  ~CommandExecutor();

  Trie* current_db;

  std::string execute(std::string);
  void handle_load(vector<string> com_vec,string &result);
  void handle_locate(vector<string> com_vec,string &result);
  void handle_new(vector<string> com_vec,string &result);
  void handle_end(vector<string> com_vec,string &result);
};

enum Com_Type{LOAD, LOCATE, NEW, END, BAD};

vector<string> fetch_word(string com, Com_Type &com_type);

// The valid string for the word.
extern string validLettersAndSymbols;

}  // namespace tutorial
}  // namespace buzzdb
