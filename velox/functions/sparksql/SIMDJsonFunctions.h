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

#include "velox/functions/prestosql/SIMDJsonFunctions.h"

namespace facebook::velox::functions {

template <typename T>
struct SIMDGetJsonObjectFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE error_code
  handleFieldTypes(simdjson_result<ondemand::value> rawRes, std::string* res) {
    switch (rawRes.type()) {
      case ondemand::json_type::number: {
        std::stringstream ss;
        switch (rawRes.get_number_type()) {
          case ondemand::number_type::unsigned_integer: {
            uint64_t numRes;
            auto error = rawRes.get_uint64().get(numRes);
            if (!error) {
              ss << num_res;
              *res = ss.str();
            }
            return error;
          }
          case ondemand::number_type::signed_integer: {
            int64_t numRes;
            auto error = rawRes.get_int64().get(numRes);
            if (!error) {
              ss << numRes;
              *res = ss.str();
            }
            return error;
          }
          case ondemand::number_type::floating_point_number: {
            double numRes;
            auto error = rawRes.get_double().get(numRes);
            if (!error) {
              ss << numRes;
              *res = ss.str();
            }
            return error;
          }
        }
      }
      case ondemand::json_type::string: {
        std::string_view resView;
        auto error = rawRes.get_string().get(resView);
        *res = std::string(resView);
        return error;
      }
      case ondemand::json_type::boolean: {
        bool boolRes = false;
        raw_res.get_bool().get(boolRes);
        if (boolRes) {
          *res = "true";
        } else {
          *res = "false";
        }
        return error_code::SUCCESS;
      }
      case ondemand::json_type::object: {
        // For nested case, e.g., for "{"my": {"hello": 10}}", "$.my" will
        // return an object type.
        auto obj = rawRes.get_object();
        // For the case that result is a json object.
        std::stringstream ss;
        ss << obj;
        *res = ss.str();
        return error_code::SUCCESS;
      }
      case ondemand::json_type::array: {
        auto arrayObj = rawRes.get_array();
        // For the case that result is a json object.
        std::stringstream ss;
        ss << arrayObj;
        *res = ss.str();
        return error_code::SUCCESS;
      }
      case ondemand::json_type::null: {
        return error_code::UNSUPPORTED_ARCHITECTURE;
      }
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Json>& json,
      const arg_type<Varchar>& jsonPath) {
    ParserContext ctx(json.data(), json.size());
    try {
      ctx.parseDocument();
    } catch (simdjson_error& e) {
      return false;
    }

    // Makes a conversion from spark's json path, e.g., "$.a.b".
    char formattedJsonPath[jsonPath.size() + 1];
    int j = 0;
    for (int i = 0; i < jsonPath.length(); i++) {
      if (jsonPath[i] == '$' || jsonPath[i] == ']' || jsonPath[i] == '\'') {
        continue;
      } else if (jsonPath[i] == '[' || jsonPath[i] == '.') {
        formattedJsonPath[j] = '/';
        j++;
      } else {
        formattedJsonPath[j] = jsonPath[i];
        j++;
      }
    }
    formattedJsonPath[j] = '\0';

    auto rawRes = jsonDoc.at_pointer(formattedJsonPath);
    // Field not found.
    if (rawRes.error() == error_code::NO_SUCH_FIELD) {
      return false;
    }
    std::string res;
    error = handleFieldTypes(raw_res, &res);
    if (error) {
      return false;
    }

    result.resize(res.length());
    *result.data() = res.data();
    return true;
  }
}

} // namespace facebook::velox::functions
