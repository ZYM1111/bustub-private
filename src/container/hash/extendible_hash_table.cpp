//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.emplace_back(std::make_shared<Bucket>(bucket_size_, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::UpdateNumBuckets(int num_buckets) {
  num_buckets_ = num_buckets;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto bucket_id = IndexOf(key);
  auto bucket_tar = &dir_[bucket_id];
  return bucket_tar->get()->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto bucket_id = IndexOf(key);
  V value;
  if (!dir_[bucket_id]->Find(key, value)) {
    return false;
  }
  dir_[bucket_id]->Remove(key);
  return true;
}

// 外层是vector 里面放bucket ptr
// bucket是list<pair<k,v>>
template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // std::lock_guard<std::mutex> lock(latch_);
  auto bucket_id = IndexOf(key);
  if (bucket_id >= dir_.size()) {
    return;
  }
  auto bucket_tar = dir_[bucket_id];
  while (bucket_tar->IsFull()) {
    if (GetGlobalDepth() == GetLocalDepth(bucket_id)) {  // 目录扩张+桶分裂
      auto old_sz = static_cast<int>(dir_.size());
      IncrementGlobalDepth();
      dir_.resize(dir_.size() << 1);
      for (auto i = old_sz, j = 0; i < static_cast<int>(dir_.size()); i++, j++) {
        dir_[i] = dir_[j];
      }
      bucket_tar->IncrementDepth();
      dir_[bucket_id + old_sz] = std::make_shared<Bucket>(GetBucketSize(), bucket_tar->GetDepth());
      bucket_size_++;
      auto &old_list = bucket_tar->GetItems();
      auto &new_list = dir_[bucket_id + old_sz].get()->GetItems();
      for (auto it = old_list.begin(); it != old_list.end();) {
        auto tmp_id = IndexOf(it->first);

        if (tmp_id != bucket_id) {
          new_list.emplace_back(*it);
          it = old_list.erase(it);
        } else {
          ++it;
        }
      }
    } else {  // 桶分裂
      auto old_list = dir_[bucket_id]->GetItems();
      auto mask = (1 << dir_[bucket_id]->GetDepth()) - 1;
      for (size_t i = 0; i < dir_.size(); i++) {
        if ((i & mask) == (bucket_id & mask)) {
          dir_[i] = std::make_shared<Bucket>(GetBucketSize(), global_depth_);
          if (i != bucket_id) {
            bucket_size_++;
          }
        }
      }
      for (auto [k, v] : old_list) {
        auto id = IndexOf(k);

        dir_[id]->Insert(k, v);
      }
    }
    bucket_id = IndexOf(key);
    if (bucket_id >= dir_.size()) {
      return;
    }
    bucket_tar = dir_[bucket_id];
  }
  bucket_tar->Insert(key, value);
  bucket_tar = nullptr;
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  auto &list = GetItems();
  for (const auto &pa : list) {
    if (pa.first == key) {
      value = pa.second;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  auto &list = GetItems();
  auto it = list.begin();
  for (; it != list.end(); ++it) {
    if (it->first == key) {
      break;
    }
  }
  if (it == list.end()) {
    return false;
  }
  list.erase(it);
  return true;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  auto &list = GetItems();
  if (IsFull()) {
    return false;
  }
  V tmp;
  if (Find(key, tmp) && !Remove(key)) {
    return false;
  }
  list.emplace_back(std::make_pair(key, value));
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
