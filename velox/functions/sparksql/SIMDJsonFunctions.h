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

using namespace simdjson;

namespace facebook::velox::functions {

template <typename T>
struct SIMDGetJsonObjectFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE simdjson::error_code handleFieldTypes(
      simdjson_result<simdjson::fallback::ondemand::value> rawRes,
      std::string* res) {
    switch (rawRes.type()) {
      case ondemand::json_type::number: {
        std::stringstream ss;
        switch (rawRes.get_number_type()) {
          case ondemand::number_type::unsigned_integer: {
            uint64_t numRes;
            auto error = rawRes.get_uint64().get(numRes);
            if (!error) {
              ss << numRes;
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
        rawRes.get_bool().get(boolRes);
        if (boolRes) {
          *res = "true";
        } else {
          *res = "false";
        }
        return simdjson::SUCCESS;
      }
      case ondemand::json_type::object: {
        // For nested case, e.g., for "{"my": {"hello": 10}}", "$.my" will
        // return an object type.
        auto obj = rawRes.get_object();
        // For the case that result is a json object.
        std::stringstream ss;
        ss << obj;
        *res = ss.str();
        return simdjson::SUCCESS;
      }
      case ondemand::json_type::array: {
        auto arrayObj = rawRes.get_array();
        // For the case that result is a json object.
        std::stringstream ss;
        ss << arrayObj;
        *res = ss.str();
        return simdjson::SUCCESS;
      }
      case ondemand::json_type::null: {
        return simdjson::UNSUPPORTED_ARCHITECTURE;
      }
    }
  }

  // This is simple validation by checking whether the obtained result is
  // followed by expected char. It is useful in ondemand kind of parsing which
  // can ignore the validation of character following closing '"'. This functon
  // is a simple checking. For many cases, even though it returns true, the raw
  // json string can still be illegal possibly.
  FOLLY_ALWAYS_INLINE bool isValidEnding(
      const char* current_position,
      int check_index) {
    char ending_char = current_position[check_index];
    if (ending_char == ',') {
      return true;
    } else if (ending_char == '}') {
      return true;
    } else if (ending_char == ']') {
      return true;
    } else if (
        ending_char == ' ' || ending_char == '\r' || ending_char == '\n' ||
        ending_char == '\t') {
      // space, '\r', '\n' or '\t' can precede valid ending char.
      return isValidEnding(current_position, check_index + 1);
    } else {
      return false;
    }
  }

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& json,
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
    for (int i = 0; i < jsonPath.size(); i++) {
      if (jsonPath.data()[i] == '$' || jsonPath.data()[i] == ']' ||
          jsonPath.data()[i] == '\'') {
        continue;
      } else if (jsonPath.data()[i] == '[' || jsonPath.data()[i] == '.') {
        formattedJsonPath[j] = '/';
        j++;
      } else {
        formattedJsonPath[j] = jsonPath.data()[i];
        j++;
      }
    }
    formattedJsonPath[j] = '\0';

    simdjson_result<simdjson::fallback::ondemand::value> rawRes;
    try {
      rawRes = ctx.jsonDoc.at_pointer(formattedJsonPath);
    } catch (...) {
      return false;
    }
    // Field not found.
    if (rawRes.error() == NO_SUCH_FIELD) {
      return false;
    }
    std::string res;
    try {
      auto error = handleFieldTypes(rawRes, &res);
      if (error) {
        return false;
      }
    } catch (...) {
      return false;
    }

    const char* current_location;
    ctx.jsonDoc.current_location().get(current_location);
    if (!isValidEnding(current_location, 0)) {
      return false;
    }

    result.resize(res.length());
    std::memcpy(result.data(), res.data(), res.length());
    return true;
  }
};

} // namespace facebook::velox::functions
