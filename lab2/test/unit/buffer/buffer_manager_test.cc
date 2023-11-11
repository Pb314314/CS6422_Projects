#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <cstring>
#include <memory>
#include <random>
#include <thread>
#include <vector>

#include "buffer/buffer_manager.h"
using namespace std;
namespace {

TEST(BufferManagerTest, FixSingle) {
  buzzdb::BufferManager buffer_manager{1024, 10};
  std::vector<uint64_t> expected_values(1024 / sizeof(uint64_t), 123);
  {
    auto& page = buffer_manager.fix_page(1, true);
    std::memcpy(page.get_data(), expected_values.data(), 1024);
    buffer_manager.unfix_page(page, true);
    EXPECT_EQ(std::vector<uint64_t>{1}, buffer_manager.get_fifo_list());
    EXPECT_TRUE(buffer_manager.get_lru_list().empty());
  }
  {
    std::vector<uint64_t> values(1024 / sizeof(uint64_t));
    auto& page = buffer_manager.fix_page(1, false);
    std::memcpy(values.data(), page.get_data(), 1024);
    buffer_manager.unfix_page(page, true);
    EXPECT_TRUE(buffer_manager.get_fifo_list().empty());
    EXPECT_EQ(std::vector<uint64_t>{1}, buffer_manager.get_lru_list());
    ASSERT_EQ(expected_values, values);
  }
}

TEST(BufferManagerTest, PersistentRestart) {
  auto buffer_manager = std::make_unique<buzzdb::BufferManager>(1024, 10);
  std::vector<uint64_t> values(1024 / sizeof(uint64_t));
  for (uint16_t segment = 0; segment < 3; ++segment) {
    for (uint64_t segment_page = 0; segment_page < 10; ++segment_page) {
      uint64_t page_id = (static_cast<uint64_t>(segment) << 48) | segment_page;
      auto& page = buffer_manager->fix_page(page_id, true);
      uint64_t& value = *reinterpret_cast<uint64_t*>(page.get_data());
      value = segment * 10 + segment_page;
      //uint64_t test_value = *reinterpret_cast<uint64_t*>(page.get_data());
      //cout<<"The data is:"<< test_value <<endl;
      //cout<<"The data is:"<< page.get_data()[0] <<endl;
      buffer_manager->unfix_page(page, true);
    }
  }
  // Destroy the buffer manager and create a new one.
  buffer_manager = std::make_unique<buzzdb::BufferManager>(1024, 10);
  for (uint16_t segment = 0; segment < 3; ++segment) {
    for (uint64_t segment_page = 0; segment_page < 10; ++segment_page) {
      uint64_t page_id = (static_cast<uint64_t>(segment) << 48) | segment_page;
      auto& page = buffer_manager->fix_page(page_id, false);
      uint64_t value = *reinterpret_cast<uint64_t*>(page.get_data());
      buffer_manager->unfix_page(page, false);
      EXPECT_EQ(segment * 10 + segment_page, value);
    }
  }
}

TEST(BufferManagerTest, FIFOEvict) {
  buzzdb::BufferManager buffer_manager{1024, 10};
  for (uint64_t i = 1; i < 11; ++i) {
    auto& page = buffer_manager.fix_page(i, false);
    buffer_manager.unfix_page(page, false);
  }
  {
    std::vector<uint64_t> expected_fifo{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    EXPECT_EQ(expected_fifo, buffer_manager.get_fifo_list());
    EXPECT_TRUE(buffer_manager.get_lru_list().empty());
  }
  {
    auto& page = buffer_manager.fix_page(11, false);
    buffer_manager.unfix_page(page, false);
  }
  {
    std::vector<uint64_t> expected_fifo{2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
    EXPECT_EQ(expected_fifo, buffer_manager.get_fifo_list());
    EXPECT_TRUE(buffer_manager.get_lru_list().empty());
  }
}

TEST(BufferManagerTest, Pb_Valgrind_test) {
  buzzdb::BufferManager buffer_manager{1024, 10};
  for (uint64_t i = 1; i < 10; ++i) {
    auto& page = buffer_manager.fix_page(i, false);
    buffer_manager.unfix_page(page, false);
    auto& page1 = buffer_manager.fix_page(i, false);
    buffer_manager.unfix_page(page1, false);
  }
  
}

TEST(BufferManagerTest, BufferFull) {
  buzzdb::BufferManager buffer_manager{1024, 10};
  std::vector<buzzdb::BufferFrame*> pages;
  pages.reserve(10);
  for (uint64_t i = 1; i < 11; ++i) {
    auto& page = buffer_manager.fix_page(i, false);
    pages.push_back(&page);
  }
  EXPECT_THROW(buffer_manager.fix_page(11, false), buzzdb::buffer_full_error);
  for (auto* page : pages) {
    buffer_manager.unfix_page(*page, false);
  }
}

TEST(BufferManagerTest, MoveToLRU) {
  buzzdb::BufferManager buffer_manager{1024, 10};
  auto& fifo_page = buffer_manager.fix_page(1, false);
  auto* lru_page = &buffer_manager.fix_page(2, false);
  buffer_manager.unfix_page(fifo_page, false);
  buffer_manager.unfix_page(*lru_page, false);
  EXPECT_EQ((std::vector<uint64_t>{1, 2}), buffer_manager.get_fifo_list());
  EXPECT_TRUE(buffer_manager.get_lru_list().empty());
  lru_page = &buffer_manager.fix_page(2, false);
  buffer_manager.unfix_page(*lru_page, false);
  EXPECT_EQ(std::vector<uint64_t>{1}, buffer_manager.get_fifo_list());
  EXPECT_EQ(std::vector<uint64_t>{2}, buffer_manager.get_lru_list());
}

TEST(BufferManagerTest, LRURefresh) {
  buzzdb::BufferManager buffer_manager{1024, 10};
  auto* page1 = &buffer_manager.fix_page(1, false);
  buffer_manager.unfix_page(*page1, false);
  page1 = &buffer_manager.fix_page(1, false);
  buffer_manager.unfix_page(*page1, false);
  auto* page2 = &buffer_manager.fix_page(2, false);
  buffer_manager.unfix_page(*page2, false);
  page2 = &buffer_manager.fix_page(2, false);
  buffer_manager.unfix_page(*page2, false);
  EXPECT_TRUE(buffer_manager.get_fifo_list().empty());
  EXPECT_EQ((std::vector<uint64_t>{1, 2}), buffer_manager.get_lru_list());
  page1 = &buffer_manager.fix_page(1, false);
  buffer_manager.unfix_page(*page1, false);
  EXPECT_TRUE(buffer_manager.get_fifo_list().empty());
  EXPECT_EQ((std::vector<uint64_t>{2, 1}), buffer_manager.get_lru_list());
}

TEST(BufferManagerTest, MultithreadParallelFix) {
  buzzdb::BufferManager buffer_manager{1024, 10};
  std::vector<std::thread> threads;
  for (size_t i = 0; i < 4; ++i) {
    threads.emplace_back([i, &buffer_manager] {
      auto& page1 = buffer_manager.fix_page(i, false);
      auto& page2 = buffer_manager.fix_page(i + 4, false);
      buffer_manager.unfix_page(page1, false);
      buffer_manager.unfix_page(page2, false);
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  auto fifo_list = buffer_manager.get_fifo_list();
  std::sort(fifo_list.begin(), fifo_list.end());
  std::vector<uint64_t> expected_fifo{0, 1, 2, 3, 4, 5, 6, 7};
  EXPECT_EQ(expected_fifo, fifo_list);
  EXPECT_TRUE(buffer_manager.get_lru_list().empty());
}

TEST(BufferManagerTest, MultithreadExclusiveAccess) {
  buzzdb::BufferManager buffer_manager{1024, 10};
  {
    auto& page = buffer_manager.fix_page(0, true);
    std::memset(page.get_data(), 0, 1024);
    buffer_manager.unfix_page(page, true);
  }
  std::vector<std::thread> threads;
  for (size_t i = 0; i < 4; ++i) {
    threads.emplace_back([&buffer_manager] {
      for (size_t j = 0; j < 1000; ++j) {//1000
        auto& page = buffer_manager.fix_page(0, true);
        uint64_t& value = *reinterpret_cast<uint64_t*>(page.get_data());
        ++value;
        //cout<<"Set number: "<<value<<endl;
        buffer_manager.unfix_page(page, true);
      }
      //cout<< "Finished!!"<<endl;
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_TRUE(buffer_manager.get_fifo_list().empty());
  EXPECT_EQ(std::vector<uint64_t>{0}, buffer_manager.get_lru_list());
  auto& page = buffer_manager.fix_page(0, false);
  uint64_t value = *reinterpret_cast<uint64_t*>(page.get_data());
  buffer_manager.unfix_page(page, false);
  EXPECT_EQ(4000, value);
}

TEST(BufferManagerTest, MultithreadBufferFull) {
  buzzdb::BufferManager buffer_manager{1024, 10};
  std::atomic<uint64_t> num_buffer_full = 0;
  std::atomic<uint64_t> finished_threads = 0;
  std::vector<std::thread> threads;
  for (size_t i = 0; i < 8; ++i) {
    threads.emplace_back(
        [i, &buffer_manager, &num_buffer_full, &finished_threads] {
          std::vector<buzzdb::BufferFrame*> pages;
          pages.reserve(8);
          for (size_t j = 0; j < 8; ++j) {
            try {
              pages.push_back(&buffer_manager.fix_page(i + j * 8, false));
            } catch (const buzzdb::buffer_full_error&) {
              ++num_buffer_full;
            }
          }
          ++finished_threads;
          // Busy wait until all threads have finished.
          while (finished_threads.load() < 8) {
          }
          for (auto* page : pages) {
            buffer_manager.unfix_page(*page, false);
          }
        });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(10, buffer_manager.get_fifo_list().size());
  EXPECT_TRUE(buffer_manager.get_lru_list().empty());
  EXPECT_EQ(54, num_buffer_full.load());
}

TEST(BufferManagerTest, Pb_LRU_test) {
  auto buffer_manager = std::make_unique<buzzdb::BufferManager>(1024, 10);
  std::vector<uint64_t> values(1024 / sizeof(uint64_t));
  for (uint16_t segment = 0; segment < 1; ++segment) {
    for (uint64_t segment_page = 0; segment_page < 10; ++segment_page) {
      uint64_t page_id = (static_cast<uint64_t>(segment) << 48) | segment_page;
      auto& page = buffer_manager->fix_page(page_id, true);
      buffer_manager->unfix_page(page, true);
      auto& page2 = buffer_manager->fix_page(page_id, true);
      buffer_manager->unfix_page(page2, true);
    }
  }
  for (uint16_t segment = 0; segment < 3; ++segment) {
    for (uint64_t segment_page = 0; segment_page < 10; ++segment_page) {
      uint64_t page_id = (static_cast<uint64_t>(segment) << 48) | segment_page;
      auto& page = buffer_manager->fix_page(page_id, true);
      buffer_manager->unfix_page(page, true);
      auto& page2 = buffer_manager->fix_page(page_id, true);
      buffer_manager->unfix_page(page2, true);
    }
  }
}

TEST(BufferManagerTest, PbManyPages) {
  buzzdb::BufferManager buffer_manager{1024, 10};
  
  // Lambda function for your task
  auto task = [&buffer_manager] {
    std::mt19937_64 engine{1};
    std::geometric_distribution<uint64_t> distr{0.1};
    for (size_t j = 0; j < 100; ++j) {
      auto& page = buffer_manager.fix_page(distr(engine), false);
      buffer_manager.unfix_page(page, false);
    }
  };
  
  // Call the lambda function
  task();
  
  // You can add more calls to the lambda function if needed...
}

TEST(BufferManagerTest, MultithreadManyPages) {
  buzzdb::BufferManager buffer_manager{1024, 10};
  std::vector<std::thread> threads;
  for (size_t i = 0; i < 4; ++i) {
    threads.emplace_back([i, &buffer_manager] {
      std::mt19937_64 engine{i};
      std::geometric_distribution<uint64_t> distr{0.1};
      for (size_t j = 0; j < 10000; ++j) {
        auto& page = buffer_manager.fix_page(distr(engine), false);
        buffer_manager.unfix_page(page, false);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

TEST(BufferManagerTest, MultithreadReaderWriter) {
  {
    // Zero out all pages first
    buzzdb::BufferManager buffer_manager{1024, 10};
    for (uint16_t segment = 0; segment <= 3; ++segment) {
      for (uint64_t segment_page = 0; segment_page <= 100; ++segment_page) {
        uint64_t page_id =
            (static_cast<uint64_t>(segment) << 48) | segment_page;
        auto& page = buffer_manager.fix_page(page_id, true);
        std::memset(page.get_data(), 0, 1024);
        buffer_manager.unfix_page(page, true);
      }
    }
    // Let the buffer manager be destroyed here so that the caches are
    // empty before running the actual test.
  }
  buzzdb::BufferManager buffer_manager{1024, 10};
  std::atomic<size_t> aborts = 0;
  std::vector<std::thread> threads;
  for (size_t i = 0; i < 4; ++i) {
    threads.emplace_back([i, &buffer_manager, &aborts] {
      std::mt19937_64 engine{i};
      // 5% of queries are scans.
      std::bernoulli_distribution scan_distr{0.05};
      // Number of pages accessed by a point query is geometrically
      // distributed.
      std::geometric_distribution<size_t> num_pages_distr{0.5};
      // 60% of point queries are reads.
      std::bernoulli_distribution reads_distr{0.6};
      // Out of 20 accesses, 12 are from segment 0, 5 from segment 1,
      // 2 from segment 2, and 1 from segment 3.
      std::discrete_distribution<uint16_t> segment_distr{12.0, 5.0, 2.0, 1.0};
      // Page accesses for point queries are uniformly distributed in
      // [0, 100].
      std::uniform_int_distribution<uint64_t> page_distr{0, 100};
      std::vector<uint64_t> scan_sums(4);
      for (size_t j = 0; j < 100; ++j) {
        uint16_t segment = segment_distr(engine);
        uint64_t segment_shift = static_cast<uint64_t>(segment) << 48;
        if (scan_distr(engine)) {
          // scan
          uint64_t scan_sum = 0;
          for (uint64_t segment_page = 0; segment_page <= 100; ++segment_page) {
            uint64_t page_id = segment_shift | segment_page;
            buzzdb::BufferFrame* page;
            while (true) {
              try {
                page = &buffer_manager.fix_page(page_id, false);
                break;
              } catch (const buzzdb::buffer_full_error&) {
                // Don't abort scan when the buffer is full, retry
                // the current page.
              }
            }
            uint64_t value = *reinterpret_cast<uint64_t*>(page->get_data());
            scan_sum += value;
            buffer_manager.unfix_page(*page, false);
          }
          EXPECT_GE(scan_sum, scan_sums[segment]);
          scan_sums[segment] = scan_sum;
        } else {
          // point query
          auto num_pages = num_pages_distr(engine) + 1;
          // For point queries all accesses but the last are always
          // reads. Only the last is potentially a write. Also,
          // all pages but the last are held for the entire duration
          // of the query.
          std::vector<buzzdb::BufferFrame*> pages;
          auto unfix_pages = [&] {
            for (auto it = pages.rbegin(); it != pages.rend(); ++it) {
              auto& page = **it;
              buffer_manager.unfix_page(page, false);
            }
            pages.clear();
          };
          for (size_t page_number = 0; page_number < num_pages - 1;
               ++page_number) {
            uint64_t segment_page = page_distr(engine);
            uint64_t page_id = segment_shift | segment_page;
            buzzdb::BufferFrame* page;
            try {
              page = &buffer_manager.fix_page(page_id, false);
            } catch (const buzzdb::buffer_full_error&) {
              // Abort query when buffer is full.
              ++aborts;
              goto abort;
            }
            pages.push_back(page);
          }
          // Unfix all pages before accessing the last one
          // (potentially exclusively) to avoid deadlocks.
          unfix_pages();
          {
            uint64_t segment_page = page_distr(engine);
            uint64_t page_id = segment_shift | segment_page;
            if (reads_distr(engine)) {
              // read
              buzzdb::BufferFrame* page;
              try {
                page = &buffer_manager.fix_page(page_id, false);
              } catch (const buzzdb::buffer_full_error&) {
                ++aborts;
                goto abort;
              }
              buffer_manager.unfix_page(*page, false);
            } else {
              // write
              buzzdb::BufferFrame* page;
              try {
                page = &buffer_manager.fix_page(page_id, true);
              } catch (const buzzdb::buffer_full_error&) {
                ++aborts;
                goto abort;
              }
              auto& value = *reinterpret_cast<uint64_t*>(page->get_data());
              ++value;
              buffer_manager.unfix_page(*page, true);
            }
          }
        abort:
          unfix_pages();
        }
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_LT(aborts.load(), 20);
}

}  // namespace

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
