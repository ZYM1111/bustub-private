//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <utility>
#include "common/config.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  if (Size() == 0) {
    return false;
  }
  for (auto [ti, id] : history_time_id_) {
    if (history_is_evictable_.count(id) != 0U) {
      *frame_id = id;
      history_id_time_.erase(std::make_pair(id, ti));
      history_time_id_.erase(std::make_pair(ti, id));
      history_cnt_[id] = 0;
      history_is_evictable_.erase(id);
      total_.erase(id);
      return true;
    }
  }

  for (auto [ti, id] : cache_time_id_) {
    if (cache_is_evictable_.count(id) != 0U) {
      *frame_id = id;
      cache_id_time_.erase(std::make_pair(id, ti));
      cache_time_id_.erase(std::make_pair(ti, id));
      cache_is_evictable_.erase(id);
      total_.erase(id);
      return true;
    }
  }
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  auto it = cache_id_time_.lower_bound(std::make_pair(frame_id, 0));
  if (it != cache_id_time_.end() && it->first == frame_id) {  // 在cache queue里, 更新时间
    auto [id, ti] = *it;
    auto it2 = cache_time_id_.lower_bound(std::make_pair(ti, id));
    cache_id_time_.erase(it);
    cache_time_id_.erase(it2);
    cache_id_time_.insert(std::make_pair(id, current_timestamp_));
    cache_time_id_.insert(std::make_pair(current_timestamp_, id));
    current_timestamp_ += 1;
  } else {
    it = history_id_time_.lower_bound(std::make_pair(frame_id, 0));
    if (it != history_id_time_.end() && it->first == frame_id) {  // 在history queue里
      auto [id, ti] = *it;
      auto it2 = history_time_id_.lower_bound(std::make_pair(ti, id));
      history_id_time_.erase(it);
      history_time_id_.erase(it2);

      history_cnt_[frame_id]++;
      if (history_cnt_[frame_id] == k_) {  // his queue里删除, 添加到cache queue
        history_cnt_[frame_id] = 0;
        if (cache_is_evictable_.size() == replacer_size_) {  // cache满了, 删一个
          for (auto it3 : cache_time_id_) {
            if (cache_is_evictable_.count(it3.second) != 0U) {
              cache_is_evictable_.erase(it3.second);
              total_.erase(it3.second);
              cache_id_time_.erase(std::make_pair(it3.second, it3.first));
              cache_time_id_.erase(std::make_pair(it3.first, it3.second));
              break;
            }
          }
        }
        cache_id_time_.insert(std::make_pair(id, current_timestamp_));
        cache_time_id_.insert(std::make_pair(current_timestamp_, id));
        current_timestamp_ += 1;

        if (history_is_evictable_.count(id) != 0U) {
          history_is_evictable_.erase(id);
          cache_is_evictable_.insert(id);
        }
      } else {  // his queue中更新时间
        history_id_time_.insert(std::make_pair(id, current_timestamp_));
        history_time_id_.insert(std::make_pair(current_timestamp_, id));
        current_timestamp_ += 1;
      }
    } else {                                                 // 不在cache, 也不在his中
      if (history_is_evictable_.size() == replacer_size_) {  // his queue满了
        for (auto [ti, id] : history_time_id_) {
          if (history_is_evictable_.count(id) != 0U) {
            history_is_evictable_.erase(id);
            history_id_time_.erase(std::make_pair(id, ti));
            history_time_id_.erase(std::make_pair(ti, id));
            history_cnt_[id] = 0;
            break;
          }
        }
      }
      history_cnt_[frame_id] = 1;
      history_time_id_.insert(std::make_pair(current_timestamp_, frame_id));
      history_id_time_.insert(std::make_pair(frame_id, current_timestamp_));
      history_is_evictable_.insert(frame_id);  // 默认可以删除
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  auto it = history_id_time_.lower_bound(std::make_pair(frame_id, 0));
  if (it != history_id_time_.end() && it->first == frame_id) {
    if (set_evictable) {
      history_is_evictable_.insert(frame_id);
    } else {
      history_is_evictable_.erase(frame_id);
    }
  }
  if (set_evictable) {
    cache_is_evictable_.insert(frame_id);
  } else {
    cache_is_evictable_.erase(frame_id);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {}

auto LRUKReplacer::Size() -> size_t { return history_is_evictable_.size() + cache_is_evictable_.size(); }

}  // namespace bustub
