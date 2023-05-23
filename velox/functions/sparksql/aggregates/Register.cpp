/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/functions/sparksql/aggregates/Register.h"
#include "velox/functions/sparksql/aggregates/BitwiseXorAggregate.h"
#include "velox/functions/sparksql/aggregates/BloomFilterAggAggregate.h"
#include "velox/functions/sparksql/aggregates/DecimalAvgAggregate.h"
#include "velox/functions/sparksql/aggregates/DecimalSumAggregate.h"

namespace facebook::velox::functions::aggregate::sparksql {

using namespace facebook::velox::functions::sparksql::aggregates;

extern void registerFirstLastAggregates(const std::string& prefix);

void registerAggregateFunctions(const std::string& prefix) {
  registerFirstLastAggregates(prefix);
  registerBitwiseXorAggregate(prefix + "bit_xor");
  registerBloomFilterAggAggregate(prefix + "bloom_filter_agg");
  registerDecimalAvgAggregate(prefix + "decimal_avg");
  registerDecimalSumAggregate(prefix + "decimal_sum");
}
} // namespace facebook::velox::functions::aggregate::sparksql
