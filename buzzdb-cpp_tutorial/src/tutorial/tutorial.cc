/*******************************************************
 * File: trie.cpp
 * Author: Bo Pang
 * GT ID: 903924447
 * Email: bpang42@gatech.edu
 * 
 * Description:
 * This file contains the implementation of CS6422 Assignment1;
 * Impliment the CommandExecutor structure;
 *******************************************************/

#include <iostream>
#include <vector>
#include <fstream>
#include <sstream>
#include <algorithm>
#include "tutorial/tutorial.h"


using namespace std;
namespace buzzdb {
namespace tutorial {

/// Your implementation go here

string validLettersAndSymbols = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'";
// Used to split the command into vector and divide the commands into four groups
// Input: com: the command info. com_type: the type of the given command 
vector<string> fetch_word(string com, Com_Type & com_type){
    vector<string> result;
    string current = "";
    for(int i=0;i<(int)com.size();i++){
        //使用isalpha 分割 会出问题
        /*if(isalpha(com[i]) ){
            current+=com[i];
        }
        else{
            if(!current.empty()){
                result.push_back(current);
                current.clear();
            }
        }*/
        //使用isspace分割
        if(isspace(com[i])){
            if(!current.empty()){
                result.push_back(current);
                current.clear();
            }
        }
        else{
            current+=com[i];
        }
    }
    if(!current.empty()){
        result.push_back(current);
        current.clear();
    }
    if(result.empty()){
        com_type = BAD;
        return result;
    }
    if(result[0] == "load") com_type = LOAD;
    else if(result[0] == "locate") com_type = LOCATE;
    else if(result[0] == "new") com_type = NEW;
    else if(result[0] == "end") com_type = END;
    else com_type = BAD;
    return result;
}

//Constructor and destructor for class CommandExecutor
CommandExecutor::CommandExecutor(): current_db(new Trie()){}
CommandExecutor::~CommandExecutor(){
    if(current_db){
        current_db->ReleaseMemory(current_db->root);
        delete(current_db);
        current_db = nullptr;
    } 
}

// The execute function: divide the command and run different handle function.
// Input: The given command;
// Output: The return string
std::string CommandExecutor::execute(std::string command) {
    //UNUSED(command);、
    //parsing the command
    for(char &c : command){
        c = tolower(c);
    }
    Com_Type com_type;
    vector<string> parsing_com = fetch_word(command,com_type);
    string result;
    switch(com_type){
        case Com_Type::LOAD:
            handle_load(parsing_com,result);
            break;
        case Com_Type::LOCATE:
            handle_locate(parsing_com,result);
            break;
        case Com_Type::NEW:
            handle_new(parsing_com,result);
            break;
        case Com_Type::END:
            handle_end(parsing_com,result);
            break;
        default:
            // ERROR: Invalid command
            result = "ERROR: Invalid command";
            break;
    }
    return result;
}

// Use to load the given file and insert words into Trie.
// Input: com_vec: contain the command info. result: Record the position info.
void CommandExecutor::handle_load(vector<string> com_vec, string &result){
    if(com_vec.size() == 1 || com_vec.size() != 2){
        //size error
        result = "ERROR: Invalid command";
        return;
    }
    std::string filename = com_vec[1];
    std::ifstream inputFile(filename);
    if (!inputFile.is_open()) {
        std::cerr << "Failed to open the file: " << filename << std::endl;
        result = "ERROR: Invalid command";
        return;
    }
    std::string line;
    if(current_db) current_db->ReleaseMemory(current_db->root);//do the cleaning stuff after the file is ready to read
    delete(current_db);
    current_db = nullptr;
    Trie* trie = new Trie;
    this->current_db = trie;
    int index = 1;
    while (std::getline(inputFile, line)) {
        if(line == "") continue;
        for(char& c:line){
            //check the line and replace all invalid letter and symbles to ' '
            if(validLettersAndSymbols.find(c) == string::npos) c = ' ';
            if(isupper(c)) c = tolower(c);
        }
        std::istringstream iss(line);
        //std::vector<std::string> words;
        std::string word;
        while (iss >> word) {
            if(!word.empty()){
                trie->insert_with_loc(word,index);//index is the index of the current word in the file.
                index++;
            }
        }
    }
    inputFile.close();
    return;
}

// Use to locate the word in the file.
// Input: com_vec: contain the command info. result: Record the position info.
void CommandExecutor::handle_locate(vector<string> com_vec,string &result){
    if(com_vec.size() != 3){
        //size error
        result = "ERROR: Invalid command";
        return;
    }
    for(char c:com_vec[2]){
        //number error
        if(c>='0' && c<='9') continue;
        result = "ERROR: Invalid command";
        return;
    }
    for(char &c:com_vec[1]){
        //valid word : only contains a-z,0-9,'
        c = tolower(c);
        if(!((c>='a'&& c<='z') || (c >='0' && c<='9') || c == '\'')){
            result = "ERROR: Invalid command";
            return;
        }
    } 
    if(!current_db){//if no files are loaded in
        result = "No matching entry";
        return;
    } 
    int ind = std::stoi(com_vec[2],nullptr,10);     //use the decimal(00002 is a valid input).
    if(ind<=0){
        result = "ERROR: Invalid command";
        return;
    }
    if(!current_db){
        return;
    }
    current_db->search_with_loc(com_vec[1], ind, result);   
    return;
}

// Handle the new command;
// Input: com_vec: contains the command info. result: contains the result.
void CommandExecutor::handle_new(vector<string> com_vec,string &result){
    if(com_vec.size() != 1){
        result = "ERROR: Invalid command";
        return;
    }
    if(current_db){     // avoid delete a deleted pointer.
        current_db->ReleaseMemory(current_db->root);
        delete(current_db);
        current_db = nullptr;
    }
    return;
}

// Handle the end command
// Input: com_vec: contains the command info. result: contains the result.
void CommandExecutor::handle_end(vector<string> com_vec,string &result){
    if(com_vec.size() != 1){
        result = "ERROR: Invalid command";
        return;
    }
    if(current_db){     // avoid delete a deleted pointer.(dangling pointer)
        current_db->ReleaseMemory(current_db->root);
        delete(current_db);
        current_db = nullptr;
    }
    return;
}

}  // namespace tutorial
}  // namespace buzzdb
