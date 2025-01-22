#include <bitset>
#include <string>
#include <vector>

#include "common/logger.h"
#include "primer/hyperloglog.h"

namespace bustub {

template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) : cardinality_(0) {
  if (n_bits < 0) n_bits = 0;
  this->n_bits_ = n_bits;
  this->buckets_ = std::vector<uint64_t>(1 << n_bits, 0);
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  return std::bitset<BITSET_CAPACITY>(hash);
}

template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  for (int i = this->n_bits_; i < BITSET_CAPACITY; i++) {
    auto index = BITSET_CAPACITY - 1 - i;
    if (bset[index] == 1) return i - this->n_bits_ + 1;
  }
  return 0;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBucket(const std::bitset<BITSET_CAPACITY> &bset) -> int {
  int n = 0;
  for (size_t i = 0; i < this->n_bits_; i++) {
    auto index = BITSET_CAPACITY - 1 - i;
    n = n + bset[index] * (1 << i);
  }
  return n;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
#if LOG_LEVEL <= LOG_LEVEL_DEBUG
  std::string item;
  if constexpr (std::is_same_v<KeyType, int64_t>) {
    item = std::to_string(val);
  } else if constexpr (std::is_same_v<KeyType, std::string>) {
    item = val;
  }
#endif

  auto hash = this->CalculateHash(val);
  auto b = this->ComputeBinary(hash);

  LOG_DEBUG("new elem: %s, hash: %lu, binary: %s", item.c_str(), hash, b.to_string().c_str());

  auto n = this->ComputeBucket(b);
  auto lmo = this->PositionOfLeftmostOne(b);
  {
    std::lock_guard<std::mutex> lock(this->buckets_lock_);

    if (lmo <= this->buckets_[n]) return;
    this->buckets_[n] = lmo;
  }

  LOG_INFO("bucket updated, bucket: %d, current: %lu", n, lmo);
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  double sum = 0.0;

#if LOG_LEVEL <= LOG_LEVEL_DEBUG
  std::string buckets_str("buckets:");
#endif

  size_t m;
  {
    std::lock_guard<std::mutex> lock(this->buckets_lock_);

    m = this->buckets_.size();

    for (auto &bucket : this->buckets_) {
      sum += 1.0 / std::pow(2, bucket);

#if LOG_LEVEL <= LOG_LEVEL_DEBUG
      buckets_str.append(" ");
      buckets_str.append(std::to_string(bucket));
#endif
    }
  }

  LOG_DEBUG("buckets: %s, sum: %.3f", buckets_str.c_str(), sum);

  if (sum <= 0) return;

  auto cardinality = static_cast<std::size_t>(std::floor(CONSTANT * m * m / sum));
  {
    std::lock_guard<std::mutex> lock(this->buckets_lock_);

    if (cardinality <= this->cardinality_) return;
    this->cardinality_ = cardinality;
  }

  LOG_DEBUG("compute cardinality: %.3f * %lu * %lu / %.3f = %lu", CONSTANT, m, m, sum, cardinality);
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
