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
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"
#include "velox/type/Type.h"

#include <stdint.h>

namespace facebook::velox::functions::sparksql::test {
namespace {

class ConcatWsTest : public SparkFunctionBaseTest {
 protected:
  std::string generateRandomString(size_t length) {
    const std::string chars =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    std::string randomString;
    for (std::size_t i = 0; i < length; ++i) {
      randomString += chars[folly::Random::rand32() % chars.size()];
    }
    return randomString;
  }

  void testConcatWsFlatVector(
      const std::vector<std::vector<std::string>>& inputTable,
      const size_t argsCount,
      const std::string& separator) {
    std::vector<VectorPtr> inputVectors;

    for (int i = 0; i < argsCount; i++) {
      inputVectors.emplace_back(
          BaseVector::create(VARCHAR(), inputTable.size(), execCtx_.pool()));
    }

    for (int row = 0; row < inputTable.size(); row++) {
      for (int col = 0; col < argsCount; col++) {
        std::static_pointer_cast<FlatVector<StringView>>(inputVectors[col])
            ->set(row, StringView(inputTable[row][col]));
      }
    }

    auto buildConcatQuery = [&]() {
      std::string output = "concat_ws('" + separator + "'";

      for (int i = 0; i < argsCount; i++) {
        output += ",c" + std::to_string(i);
      }
      output += ")";
      return output;
    };
    auto result = evaluate<FlatVector<StringView>>(
        buildConcatQuery(), makeRowVector(inputVectors));

    auto produceExpectedResult = [&](const std::vector<std::string>& inputs) {
      auto isFirst = true;
      std::string output;
      for (int i = 0; i < inputs.size(); i++) {
        auto value = inputs[i];
        if (!value.empty()) {
          if (isFirst) {
            isFirst = false;
          } else {
            output += separator;
          }
          output += value;
        }
      }
      return output;
    };

    for (int i = 0; i < inputTable.size(); ++i) {
      EXPECT_EQ(result->valueAt(i), produceExpectedResult(inputTable[i]))
          << "at " << i;
    }
  }
};

TEST_F(ConcatWsTest, columnStringArgs) {
  // Test with constant args.
  auto rows = makeRowVector(makeRowType({VARCHAR(), VARCHAR()}), 10);
  auto c0 = generateRandomString(20);
  auto c1 = generateRandomString(20);
  auto result = evaluate<SimpleVector<StringView>>(
      fmt::format("concat_ws('-', '{}', '{}')", c0, c1), rows);
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(result->valueAt(i), c0 + "-" + c1);
  }

  // Test with variable arguments.
  size_t maxArgsCount = 10;
  size_t rowCount = 100;
  size_t maxStringLength = 100;

  std::vector<std::vector<std::string>> inputTable;
  for (int argsCount = 1; argsCount <= maxArgsCount; argsCount++) {
    inputTable.clear();
    inputTable.resize(rowCount, std::vector<std::string>(argsCount));

    for (int row = 0; row < rowCount; row++) {
      for (int col = 0; col < argsCount; col++) {
        inputTable[row][col] =
            generateRandomString(folly::Random::rand32() % maxStringLength);
      }
    }

    SCOPED_TRACE(fmt::format("Number of arguments: {}", argsCount));
    testConcatWsFlatVector(inputTable, argsCount, "--testSep--");
  }
}

TEST_F(ConcatWsTest, mixedConstantAndColumnStringArgs) {
  size_t maxStringLength = 100;
  std::string value;
  auto data = makeRowVector({
      makeFlatVector<StringView>(
          1'000,
          [&](auto /* row */) {
            value =
                generateRandomString(folly::Random::rand32() % maxStringLength);
            return StringView(value);
          }),
      makeFlatVector<StringView>(
          1'000,
          [&](auto /* row */) {
            value =
                generateRandomString(folly::Random::rand32() % maxStringLength);
            return StringView(value);
          }),
  });

  auto c0 = data->childAt(0)->as<FlatVector<StringView>>()->rawValues();
  auto c1 = data->childAt(1)->as<FlatVector<StringView>>()->rawValues();

  // Test with consecutive constant inputs.
  auto result = evaluate<SimpleVector<StringView>>(
      "concat_ws('--', c0, c1, 'foo', 'bar')", data);
  auto expected = makeFlatVector<StringView>(1'000, [&](auto row) {
    value = "";
    const std::string& s0 = c0[row].str();
    const std::string& s1 = c1[row].str();

    if (s0.empty() && s1.empty()) {
      value = "foo--bar";
    } else if (!s0.empty() && !s1.empty()) {
      value = s0 + "--" + s1 + "--foo--bar";
    } else {
      value = s0 + s1 + "--foo--bar";
    }
    return StringView(value);
  });
  velox::test::assertEqualVectors(expected, result);

  // Test with non-ASCII characters.
  result = evaluate<SimpleVector<StringView>>(
      "concat_ws('$*@', 'aaa', '测试', c0, 'eee', 'ddd', c1, '\u82f9\u679c', 'fff')",
      data);
  expected = makeFlatVector<StringView>(1'000, [&](auto row) {
    value = "";
    std::string delim = "$*@";
    const std::string& s0 =
        c0[row].str().empty() ? c0[row].str() : delim + c0[row].str();
    const std::string& s1 =
        c1[row].str().empty() ? c1[row].str() : delim + c1[row].str();

    value = "aaa" + delim + "测试" + s0 + delim + "eee" + delim + "ddd" + s1 +
        delim + "\u82f9\u679c" + delim + "fff";
    return StringView(value);
  });
  velox::test::assertEqualVectors(expected, result);
}

TEST_F(ConcatWsTest, arrayArgs) {
  using S = StringView;
  auto arrayVector = makeNullableArrayVector<StringView>({
      {S("red"), S("blue")},
      {S("blue"), std::nullopt, S("yellow"), std::nullopt, S("orange")},
      {},
      {std::nullopt},
      {S("red"), S("purple"), S("green")},
  });

  // One array arg.
  auto result = evaluate<SimpleVector<StringView>>(
      "concat_ws('----', c0)", makeRowVector({arrayVector}));
  auto expected1 = {
      S("red----blue"),
      S("blue----yellow----orange"),
      S(""),
      S(""),
      S("red----purple----green"),
  };
  velox::test::assertEqualVectors(
      makeFlatVector<StringView>(expected1), result);

  // Two array args.
  result = evaluate<SimpleVector<StringView>>(
      "concat_ws('----', c0, c1)", makeRowVector({arrayVector, arrayVector}));
  auto expected2 = {
      S("red----blue----red----blue"),
      S("blue----yellow----orange----blue----yellow----orange"),
      S(""),
      S(""),
      S("red----purple----green----red----purple----green"),
  };
  velox::test::assertEqualVectors(
      makeFlatVector<StringView>(expected2), result);
}

TEST_F(ConcatWsTest, mixedStringArrayArgs) {
  using S = StringView;
  auto arrayVector = makeNullableArrayVector<StringView>({
      {S("red"), S("blue")},
      {S("blue"), std::nullopt, S("yellow"), std::nullopt, S("orange")},
      {},
      {std::nullopt},
      {S("red"), S("purple"), S("green")},
  });

  auto result = evaluate<SimpleVector<StringView>>(
      "concat_ws('----', c0, 'foo', c1, 'bar', 'end')",
      makeRowVector({arrayVector, arrayVector}));
  auto expected = {
      S("red----blue----foo----red----blue----bar----end"),
      S("blue----yellow----orange----foo----blue----yellow----orange----bar----end"),
      S("foo----bar----end"),
      S("foo----bar----end"),
      S("red----purple----green----foo----red----purple----green----bar----end"),
  };
  velox::test::assertEqualVectors(makeFlatVector<StringView>(expected), result);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
