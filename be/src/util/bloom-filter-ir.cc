
#include "util/bloom-filter.h"

#include <stdlib.h>

#include <algorithm>

namespace impala {

void BloomFilter::BucketInsertAVX2(
    const uint32_t bucket_idx, const uint32_t hash) noexcept {
  const __m256i mask = MakeMask(hash);
  __m256i* const bucket = &reinterpret_cast<__m256i*>(directory_)[bucket_idx];
  _mm256_store_si256(bucket, _mm256_or_si256(*bucket, mask));
  // For SSE compatibility, unset the high bits of each YMM register so SSE instructions
  // dont have to save them off before using XMM registers.
  _mm256_zeroupper();
}

}
