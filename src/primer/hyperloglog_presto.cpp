#include <bitset>

#include "common/logger.h"
#include "primer/hyperloglog_presto.h"

namespace bustub {

template <typename KeyType>
HyperLogLogPresto<KeyType>::HyperLogLogPresto(int16_t n_leading_bits) : cardinality_(0) {
  if (n_leading_bits < 0) n_leading_bits = 0;
  this->n_leading_bits_ = n_leading_bits;
  this->dense_bucket_ = std::vector<std::bitset<DENSE_BUCKET_SIZE>>(1 << n_leading_bits_, 0);
}

template <typename KeyType>
auto HyperLogLogPresto<KeyType>::AddElem(KeyType val) -> void {
#if LOG_LEVEL <= LOG_LEVEL_DEBUG
  std::string item;
  if constexpr (std::is_same_v<KeyType, int64_t>) {
    item = std::to_string(val);
  } else if constexpr (std::is_same_v<KeyType, std::string>) {
    item = val;
  }
#endif

  auto hash = this->CalculateHash(val);

  auto bit = std::bitset<64>(hash);
  auto bucket_index = this->ComputeBucketIndex(bit);
  auto rmo = this->PositionOfRightMostOne(bit);

  LOG_DEBUG("new elem: %s, hash: %lu, binary: %s, bucket_index: %lu, rmo: %lu", item.c_str(), hash,
            bit.to_string().c_str(), bucket_index, rmo);

  auto current = this->GetBucketValue(bucket_index);
  if (rmo > current) {
    this->SetBucketValue(bucket_index, rmo);
  }
}

template <typename T>
auto HyperLogLogPresto<T>::ComputeCardinality() -> void {
  auto sum = this->CalBucketSum();
  auto cardinality = this->CalCardinality(sum);

  if (cardinality <= this->cardinality_) return;
  this->cardinality_ = cardinality;

  LOG_DEBUG("new cardinality set: %lu", this->cardinality_);
}

template <typename T>
auto HyperLogLogPresto<T>::ComputeBucketIndex(const std::bitset<64> &bset) -> uint64_t {
  int ret = 0;
  for (auto j = 0; j < this->n_leading_bits_; j++) {
    ret += bset[63 - j] * (1 << j);
  }
  return ret;
}

template <typename T>
auto HyperLogLogPresto<T>::PositionOfRightMostOne(const std::bitset<64> &bset) const -> uint64_t {
  for (auto i = 0; i < 64 - this->n_leading_bits_; i++) {
    if (bset[i] == 1) return i;
  }
  return 64 - this->n_leading_bits_;
};

template <typename T>
auto HyperLogLogPresto<T>::CalCardinality(double sum) -> uint64_t {
  auto m = this->dense_bucket_.size();
  auto ret = static_cast<std::size_t>(std::floor(CONSTANT * m * m / sum));
  LOG_DEBUG("cardinality = %f * %lu * %lu / %f = %lu", CONSTANT, m, m, sum, ret);
  return ret;
}

template <typename T>
auto HyperLogLogPresto<T>::CalBucketSum() -> double {
  double sum = 0;
  auto m = this->dense_bucket_.size();

  for (std::size_t i = 0; i < m; i++) {
    sum += 1.0 / std::pow(2, this->GetBucketValue(i));
  }
  return sum;
}

template <typename T>
auto HyperLogLogPresto<T>::GetBucketValue(uint64_t index) -> uint64_t {
  auto dense = this->dense_bucket_[index];

  auto itr = this->overflow_bucket_.find(index);
  if (itr == this->overflow_bucket_.end()) {
    return dense.to_ulong();
  }

  auto ret = dense.to_ulong() + ((itr->second.to_ulong()) << DENSE_BUCKET_SIZE);
  LOG_DEBUG("index: %lu, dense: %lu, overflow: %lu, total: %lu", index, dense.to_ulong(), itr->second.to_ulong(), ret);
  return ret;
}

template <typename T>
auto HyperLogLogPresto<T>::SetBucketValue(uint64_t index, uint64_t value) -> void {
  this->dense_bucket_[index] = value & 0x0F;
  auto overflow = value >> DENSE_BUCKET_SIZE;

  if (overflow <= 0) return;
  this->overflow_bucket_[index] = overflow;

  LOG_DEBUG("index: %lu, rmo: %lu(%s), dense: %s overflow: %s", index, value,
            std::bitset<64>(value).to_string().c_str(), this->dense_bucket_[index].to_string().c_str(),
            this->overflow_bucket_[index].to_string().c_str());
}

template class HyperLogLogPresto<int64_t>;
template class HyperLogLogPresto<std::string>;
}  // namespace bustub
