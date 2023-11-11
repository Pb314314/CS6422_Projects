
#include <cassert>
#include <iostream>
#include <string>
#include <memory>
#include <vector>
#include <algorithm>
#include "buffer/buffer_manager.h"
#include "common/macros.h"
#include "storage/file.h"

#define UNUSED(p)  ((void)(p))
using namespace std;

namespace buzzdb {

BufferFrame::BufferFrame(uint64_t page_id, bool exclusive, unique_ptr<char[]>&& data_array):
page_id(page_id),
segment(static_cast<uint16_t>(page_id >> 48)),
segment_id((page_id<<16)>>16),
data_array(move(data_array)),
dirty(false),
fixed(0),
fifo(true)
{
    if(exclusive) type = EXCLUSIVE;
    else type = SHARE;
}

// Get data from Bufferframe
char* BufferFrame::get_data() {
    // Add lock here
    return data_array.get();
}

// Constructor of Linked_BufferFrame
Linked_BufferFrame::Linked_BufferFrame(shared_ptr<BufferFrame>& page_ptr):
page_ptr(page_ptr),next(nullptr),prev(nullptr){}

// Constructor of BufferManager
BufferManager::BufferManager(size_t page_size, size_t page_count):
hash_table(),   // Nothing in the hash_table
fifo_head(nullptr),
fifo_tail(nullptr),
lru_head(nullptr),
lru_tail(nullptr),
page_count(page_count),
page_size(page_size),
page_num(0){}

// Deallocator of BufferManager
BufferManager::~BufferManager() {
    // Write back all pages inside buffer;
    hash_table.clear();
    shared_ptr<Linked_BufferFrame> ptr = fifo_head;
    shared_ptr<Linked_BufferFrame> temp;
    while(ptr){
        write_back_page(ptr);
        temp = ptr;
        ptr = ptr->next;
        if(temp->next)temp->next.reset();
        if(temp->prev) temp->prev.reset();
        if(temp) temp.reset();
    }
    ptr= lru_head;
    while(ptr){
        write_back_page(ptr);
        temp = ptr;
        ptr = ptr->next;
        if(temp->next)temp->next.reset();
        if(temp->prev) temp->prev.reset();
        if(temp) temp.reset();
    }
}

// Insert page to a linked list.
void BufferManager::insert_page(shared_ptr<Linked_BufferFrame>& head, shared_ptr<Linked_BufferFrame> list_ptr, bool fifo){
    shared_ptr<Linked_BufferFrame>& tail = (fifo) ? fifo_tail : lru_tail;
    if(head == nullptr){
        head = list_ptr;
        tail = list_ptr;
        list_ptr->next = nullptr;
        list_ptr->prev = nullptr;
    }
    else{
        list_ptr->next = head;
        list_ptr->prev = nullptr;
        head->prev = list_ptr;
        head = list_ptr;
    }
    if(fifo) fifo_vec.push_back(list_ptr->page_ptr->page_id);
    else{
        // Insert into lru set fifo to false
        lru_vec.push_back(list_ptr->page_ptr->page_id);
        list_ptr->page_ptr->fifo = false;
    }
    page_num++; // Add page_num
    hash_table[list_ptr->page_ptr->page_id] = list_ptr; // Insert id into hash_table
    list_ptr->page_ptr->fixed++;
    return;
}

// Delete page from a linked list;
void BufferManager::delete_page(shared_ptr<Linked_BufferFrame>& head, shared_ptr<Linked_BufferFrame> list_ptr, bool fifo){
    uint64_t page_id = list_ptr->page_ptr->page_id;
    shared_ptr<Linked_BufferFrame>& tail = (fifo) ? fifo_tail : lru_tail;
    if (!head || !list_ptr) {
        return; // Nothing to delete.
    }
    if (head == list_ptr) {
        head = list_ptr->next; // Update the head to the next node.
        if (head) {
            head->prev.reset(); // Clear the previous pointer of the new head.
        }
        else tail.reset();
    }
    else{
        if (list_ptr->prev) {
            list_ptr->prev->next = list_ptr->next; // Update the next pointer of the previous node.
        }
        else {
            // If there is no previous node, list_ptr is the new head.
            head = list_ptr->next;
            if (head) {
                head->prev.reset();
            }
        }
        if (list_ptr->next) {
            list_ptr->next->prev = list_ptr->prev; // Update the previous pointer of the next node.
        }
        else {
            // If there is no next node, list_ptr is the tail.
            tail = list_ptr->prev;
        }
    }
    
    // Remove the page_id from the vector
    if(fifo){
        delete_num_vec(fifo_vec, page_id);
    }
    else{
        delete_num_vec(lru_vec, page_id);
    }
    list_ptr->page_ptr->fixed--;
    page_num--;
    hash_table.erase(page_id);  // Remove key from the hash table
    return;
}


// Move page from fifo_queue to lru_queque;
void BufferManager::move_page(shared_ptr<Linked_BufferFrame> list_ptr){
    {
    unique_lock<mutex> my_lock(fifo_mtx);
    delete_page(fifo_head,list_ptr,true);
    }
    {
    unique_lock<mutex> my_lock(lru_mtx);
    insert_page(lru_head,list_ptr,false);
    }
}

void BufferManager::write_back_page(const shared_ptr<Linked_BufferFrame> ptr){
    uint16_t segment_value = ptr->page_ptr->segment;
    string segment_str = std::to_string(segment_value);
    // Obtain a const char* from the string
    const char* filename = segment_str.c_str();
    std::unique_ptr<File> file = File::open_file(filename, File::WRITE);
    size_t offset = ptr->page_ptr->segment_id * page_size;
    file->write_block(ptr->page_ptr->data_array.get(),offset,page_size);// Write to the disk
    return;
}

unique_ptr<char[]> BufferManager::read_disk_file(uint64_t page_id){
    uint16_t segment = static_cast<uint16_t>(page_id >> 48);
    std::string segment_str = std::to_string(segment);
    // Obtain a const char* from the string
    const char* filename = segment_str.c_str();
    uint64_t segment_id = (page_id<<16)>>16;
    size_t offset = segment_id * page_size;
    unique_ptr<buzzdb::File> file = File::open_file(filename, File::WRITE);
    unique_ptr<char []> file_data = file->read_block(offset,page_size);
    return file_data;
}

// Select page from buffer to move out;
pair<bool,bool> BufferManager::victim_page(){
    // Find page in fifo
    shared_ptr<Linked_BufferFrame> ptr = fifo_tail;
    while(ptr){
        if(ptr->page_ptr->fixed == 0){
            // Find the victim
            if(ptr->page_ptr->dirty){
                // If dirty, need to write back to disk
                write_back_page(ptr);
            }
            {
            unique_lock<mutex> my_lock(fifo_mtx);
            // Delete page from the queue
            delete_page(fifo_head,ptr,true);
            if(ptr->next)   ptr->next.reset();
            if(ptr->prev)   ptr->prev.reset();
            if(ptr)         ptr.reset();
            }
            return {true,true};
        }
        ptr = ptr->prev;
    }
    ptr = lru_tail;
    while(ptr){
        if(ptr->page_ptr->fixed == 0){
            // Find the victim
            if(ptr->page_ptr->dirty){
                write_back_page(ptr);
            }
            {
            unique_lock<mutex> my_lock(lru_mtx);
            // Delete page from the queue
            delete_page(lru_head,ptr,false);
            ptr.reset();
            }
            return {true,false};
        }
        ptr = ptr->prev;
    }
    return {false,false};
}


BufferFrame& BufferManager::fix_page(uint64_t page_id, bool exclusive){
    {
    unique_lock<mutex> my_lock(shared_mtx);
    auto it = hash_table.find(page_id);
    //find the page in hash table, the page already in the buffer
    if(it != hash_table.end()){
        if(exclusive) {// Lock the page if exclusive;
            it->second->page_ptr->mtx.lock();
        }
        shared_ptr<Linked_BufferFrame> current_ptr = it->second;
        current_ptr->page_ptr->fixed++;
        if(current_ptr->page_ptr->fifo){
            // If the page in the fifo, move the page to lru and update the list_vector
            {
                move_page(current_ptr);  
            } 
        }
        else{
            {
            unique_lock<mutex> my_lock(lru_mtx);
            // Move the page to the start of the linked list;
            delete_page(lru_head,current_ptr,false);
            insert_page(lru_head,current_ptr,false);
            }
        }
        return *(current_ptr->page_ptr); // Return the reference of the page;
    }
    // Find page in the disk;
    unique_ptr<char []> file_data = read_disk_file(page_id);
    // Need a new page;
    shared_ptr<BufferFrame> page_ptr = make_shared<BufferFrame>(page_id, exclusive,move(file_data));
    Linked_BufferFrame current_frame(page_ptr);
    shared_ptr<Linked_BufferFrame> ptr = make_shared<Linked_BufferFrame>(current_frame);
    if(exclusive) ptr->page_ptr->mtx.lock();// Lock the page if exclusive;
    // Insert a new page
    if(page_num < page_count){
        // Just insert into fifo_queue
        {
        unique_lock<mutex> my_lock(fifo_mtx);
        insert_page(fifo_head, ptr, true);// Insert into fifo_queue.
        }
        // Lock the page if exclusive;
        return *ptr->page_ptr;
    }
    // Victim a page
    pair<bool,bool> victim = victim_page(); //(victim, fifo)
    if(victim.first){
        if(victim.second)
        {
        unique_lock<mutex> my_lock(fifo_mtx);
        insert_page(fifo_head, ptr, true);// Insert into fifo_queue.
        }
        else{
        unique_lock<mutex> my_lock(lru_mtx);
        insert_page(lru_head, ptr, false);// Insert into lru_queue.
        }
        return *ptr->page_ptr;
    }
    }
    // No page can victim, throw buffer_full_error{};
    throw buffer_full_error{};
}   


void BufferManager::unfix_page(BufferFrame& page, bool is_dirty) {
    page.fixed--;
    if(is_dirty) page.dirty = true; // Set dirty if is_dirty
    if(page.type == EXCLUSIVE){
        page.mtx.unlock();
    }
    return;
}


std::vector<uint64_t> BufferManager::get_fifo_list() const {
    return fifo_vec;
}


std::vector<uint64_t> BufferManager::get_lru_list() const {
    return lru_vec;
}


void BufferManager::delete_num_vec(vector<uint64_t>& vec, uint64_t num) {
        auto it = std::find(vec.begin(), vec.end(), num);

        if (it != vec.end()) {
            vec.erase(it);
        }
    }

}  // namespace buzzdb
