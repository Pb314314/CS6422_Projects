#pragma once

#include <cstddef>
#include <cstdint>
#include <exception>
#include <vector>
#include <mutex>
#include <queue>
#include "common/macros.h"
#include <unordered_map>
#include <memory>
using namespace std;
namespace buzzdb {

enum LockType {
    SHARE,
    EXCLUSIVE,
    ORIGIN
};

class BufferFrame {
private:
    friend class BufferManager;
    // TODO: add your implementation here
    uint64_t page_id;
    uint16_t segment;
    uint64_t segment_id;
    unique_ptr<char[]> data_array;
    LockType type;  
    bool dirty;
    int fixed;
    bool fifo;
    mutex mtx;
public:
    /// Returns a pointer to this page's data.
    BufferFrame(uint64_t page_id, bool exclusive, unique_ptr<char[]>&& data_array );
    char* get_data();
};

class buffer_full_error
: public std::exception {
public:
    const char* what() const noexcept override {
        return "buffer is full";
    }
};

class Linked_BufferFrame{
    public:
        Linked_BufferFrame(shared_ptr<BufferFrame>& page_ptr);
        shared_ptr<BufferFrame> page_ptr;   // current page;
        shared_ptr<Linked_BufferFrame> next;  // point to the next page;
        shared_ptr<Linked_BufferFrame> prev;  // point to the prev page;
};

class BufferManager {
private:
    // TODO: add your implementation here
    unordered_map<uint64_t, shared_ptr<Linked_BufferFrame>> hash_table;
    mutex fifo_mtx;
    mutex lru_mtx;
    mutex shared_mtx;
    shared_ptr<Linked_BufferFrame> fifo_head;
    shared_ptr<Linked_BufferFrame> fifo_tail;
    shared_ptr<Linked_BufferFrame> lru_head;
    shared_ptr<Linked_BufferFrame> lru_tail;
    size_t page_count;
    size_t page_size;
    size_t page_num;  //current number of pages;
    vector<uint64_t> fifo_vec;
    vector<uint64_t> lru_vec;
public:
    /// Constructor.
    /// @param[in] page_size  Size in bytes that all pages will have.
    /// @param[in] page_count Maximum number of pages that should reside in
    //                        memory at the same time.
    BufferManager(size_t page_size, size_t page_count);

    /// Destructor. Writes all dirty pages to disk.
    ~BufferManager();

    // Insert Linked_BufferFrame into double linked list.
    void insert_page(shared_ptr<Linked_BufferFrame>& head, shared_ptr<Linked_BufferFrame> list_ptr, bool fifo);
    
    // Delete Linked_BufferFrame from double linked list.
    void delete_page(shared_ptr<Linked_BufferFrame>& head, shared_ptr<Linked_BufferFrame> list_ptr, bool fifo);
    
    // Move page from Fifo list to Lru list.
    void move_page(shared_ptr<Linked_BufferFrame> list_ptr);
    // Write page to disk
    void write_back_page(const shared_ptr<Linked_BufferFrame> ptr);

    unique_ptr<char[]> read_disk_file(uint64_t page_id);
    pair<bool,bool> victim_page();

    void delete_num_vec(vector<uint64_t>& vec, uint64_t num);
    /// Returns a reference to a `BufferFrame` object for a given page id. When
    /// the page is not loaded into memory, it is read from disk. Otherwise the
    /// loaded page is used.
    /// When the page cannot be loaded because the buffer is full, throws the
    /// exception `buffer_full_error`.
    /// Is thread-safe w.r.t. other concurrent calls to `fix_page()` and
    /// `unfix_page()`.
    /// @param[in] page_id   Page id of the page that should be loaded.
    /// @param[in] exclusive If `exclusive` is true, the page is locked
    ///                      exclusively. Otherwise it is locked
    ///                      non-exclusively (shared).
    BufferFrame& fix_page(uint64_t page_id, bool exclusive);

    /// Takes a `BufferFrame` reference that was returned by an earlier call to
    /// `fix_page()` and unfixes it. When `is_dirty` is / true, the page is
    /// written back to disk eventually.
    void unfix_page(BufferFrame& page, bool is_dirty);

    /// Returns the page ids of all pages (fixed and unfixed) that are in the
    /// FIFO list in FIFO order.
    /// Is not thread-safe.
    std::vector<uint64_t> get_fifo_list() const;

    /// Returns the page ids of all pages (fixed and unfixed) that are in the
    /// LRU list in LRU order.
    /// Is not thread-safe.
    std::vector<uint64_t> get_lru_list() const;

    /// Returns the segment id for a given page id which is contained in the 16
    /// most significant bits of the page id.
    static constexpr uint16_t get_segment_id(uint64_t page_id) {
        return page_id >> 48;
    }

    /// Returns the page id within its segment for a given page id. This
    /// corresponds to the 48 least significant bits of the page id.
    static constexpr uint64_t get_segment_page_id(uint64_t page_id) {
        return page_id & ((1ull << 48) - 1);
    }
};

}  // namespace buzzdb
