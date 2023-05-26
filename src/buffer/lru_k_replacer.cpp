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
#include <algorithm>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <utility>

#include "common/config.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (Size() == 0) {
    return false;
  }

  bool res = std::any_of(history_time_id_.begin(), history_time_id_.end(), [&](const std::pair<size_t, frame_id_t> it) {
    auto [ti, id] = it;
    if (history_is_evictable_.count(id) != 0U) {
      *frame_id = id;
      history_id_time_.erase(std::make_pair(id, ti));
      history_time_id_.erase(std::make_pair(ti, id));
      history_cnt_.erase(id);
      history_is_evictable_.erase(id);
      return true;
    }
    return false;
  });

  if (res) {
    return res;
  }

  res = std::any_of(cache_time_id_.begin(), cache_time_id_.end(), [&](const std::pair<size_t, frame_id_t> it) {
    auto [ti, id] = it;
    if (cache_is_evictable_.count(id) != 0U) {
      *frame_id = id;
      cache_id_time_.erase(std::make_pair(id, ti));
      cache_time_id_.erase(std::make_pair(ti, id));
      cache_is_evictable_.erase(id);
      return true;
    }
    return false;
  });
  return res;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // try {
  //   if (frame_id < 0 || frame_id >= static_cast<frame_id_t>(replacer_size_)) {
  //     throw std::runtime_error("The frame id is invalid!");
  //   }
  // } catch (const std::exception &e) {
  //   std::cerr << e.what() << '\n';
  // }

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
        history_cnt_.erase(frame_id);
        // 不可驱逐性保持一致
        if (history_is_evictable_.count(frame_id) != 0U) {
          if (cache_is_evictable_.size() == replacer_size_) {  // cache满了, 删一个
            for (auto it3 : cache_time_id_) {
              if (cache_is_evictable_.count(it3.second) != 0U) {
                cache_is_evictable_.erase(it3.second);
                cache_id_time_.erase(std::make_pair(it3.second, it3.first));
                cache_time_id_.erase(std::make_pair(it3.first, it3.second));
                break;
              }
            }
          }
          history_is_evictable_.erase(frame_id);
          cache_is_evictable_.insert(frame_id);
        }
        cache_id_time_.insert(std::make_pair(id, current_timestamp_));
        cache_time_id_.insert(std::make_pair(current_timestamp_, id));
        current_timestamp_ += 1;
      } else {  // his queue中更新时间
        history_id_time_.insert(std::make_pair(id, current_timestamp_));
        history_time_id_.insert(std::make_pair(current_timestamp_, id));
        current_timestamp_ += 1;
      }
    } else {  // 不在cache, 也不在his中
      // 默认不可以删除, 所以his queue的size不会变大, 不用判断要不要删除
      history_cnt_[frame_id] = 1;
      history_time_id_.insert(std::make_pair(current_timestamp_, frame_id));
      history_id_time_.insert(std::make_pair(frame_id, current_timestamp_));
      current_timestamp_ += 1;
      // history_is_evictable_.insert(frame_id);  // 默认不可以删除
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  // try {
  //   if (frame_id < 0 || frame_id >= static_cast<frame_id_t>(replacer_size_)) {
  //     throw std::runtime_error("The frame id is invalid!");
  //   }
  // } catch (const std::exception &e) {
  //   std::cerr << e.what() << '\n';
  // }

  auto it = history_id_time_.lower_bound(std::make_pair(frame_id, 0));
  if (it != history_id_time_.end() && it->first == frame_id) {
    if (set_evictable && history_is_evictable_.count(frame_id) == 0U) {
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
      history_is_evictable_.insert(frame_id);
    } else if (!set_evictable) {
      history_is_evictable_.erase(frame_id);
    }
    return;
  }
  it = cache_id_time_.lower_bound(std::make_pair(frame_id, 0));
  if (it != cache_id_time_.end() && it->first == frame_id) {
    if (set_evictable && cache_is_evictable_.count(frame_id) == 0U) {
      if (cache_is_evictable_.size() == replacer_size_) {  // cache满了, 删一个
        for (auto it : cache_time_id_) {
          if (cache_is_evictable_.count(it.second) != 0U) {
            cache_is_evictable_.erase(it.second);
            cache_id_time_.erase(std::make_pair(it.second, it.first));
            cache_time_id_.erase(std::make_pair(it.first, it.second));
            break;
          }
        }
      }
      cache_is_evictable_.insert(frame_id);
    } else if (!set_evictable) {
      cache_is_evictable_.erase(frame_id);
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // try {
  //   if (frame_id < 0 || frame_id >= static_cast<frame_id_t>(replacer_size_)) {
  //     throw std::runtime_error("The frame id is invalid!");
  //   }
  // } catch (const std::exception &e) {
  //   std::cerr << e.what() << '\n';
  // }
  auto it = history_id_time_.lower_bound(std::make_pair(frame_id, 0));
  if (it != history_id_time_.end() && it->first == frame_id) {
    try {
      if (history_is_evictable_.count(frame_id) == 0U) {
        throw std::runtime_error("The frame is not evictable!");
      }
      history_is_evictable_.erase(frame_id);
      history_cnt_[frame_id] = 0;
      history_time_id_.erase(std::make_pair(it->second, it->first));
      history_id_time_.erase(it);
    } catch (const std::exception &e) {
      std::cerr << e.what() << '\n';
    }
    return;
  }
  it = cache_id_time_.lower_bound(std::make_pair(frame_id, 0));
  if (it != cache_id_time_.end() && it->first == frame_id) {
    try {
      if (cache_is_evictable_.count(frame_id) == 0U) {
        throw std::runtime_error("The frame is not evictable!");
      }
      cache_is_evictable_.erase(frame_id);
      cache_time_id_.erase(std::make_pair(it->second, it->first));
      cache_id_time_.erase(it);
    } catch (const std::exception &e) {
      std::cerr << e.what() << '\n';
    }
    return;
  }
}

auto LRUKReplacer::Size() -> size_t { return history_is_evictable_.size() + cache_is_evictable_.size(); }

}  // namespace bustub
