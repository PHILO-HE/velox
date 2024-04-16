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
        if (isFirst) {
          isFirst = false;
        } else {
          output += separator;
        }
        output += value;
      }
      return output;
    };

    for (int i = 0; i < inputTable.size(); ++i) {
      EXPECT_EQ(result->valueAt(i), produceExpectedResult(inputTable[i]))
          << "at " << i;
    }
  }
};

TEST_F(ConcatWsTest, stringArgs) {
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
    // Test with empty separator.
    testConcatWsFlatVector(inputTable, argsCount, "");
  }
}

TEST_F(ConcatWsTest, mixedConstantAndNonconstantStringArgs) {
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
    const std::string& s0 = c0[row].str();
    const std::string& s1 = c1[row].str();
    value = s0 + "--" + s1 + "--foo--bar";
    return StringView(value);
  });
  velox::test::assertEqualVectors(expected, result);

  // Test with non-ASCII characters.
  result = evaluate<SimpleVector<StringView>>(
      "concat_ws('$*@', 'aaa', 'åæ', c0, 'eee', 'ddd', c1, '\u82f9\u679c', 'fff')",
      data);
  expected = makeFlatVector<StringView>(1'000, [&](auto row) {
    std::string delim = "$*@";

    value = "aaa" + delim + "åæ" + delim + c0[row].str() + delim + "eee" +
        delim + "ddd" + delim + c1[row].str() + delim + "\u82f9\u679c" + delim +
        "fff";
    return StringView(value);
  });
  velox::test::assertEqualVectors(expected, result);
}

TEST_F(ConcatWsTest, arrayArgs) {
  auto arrayVector = makeNullableArrayVector<StringView>({
      {"red", "blue"},
      {"blue", std::nullopt, "yellow", std::nullopt, "orange"},
      {},
      {std::nullopt},
      {"red", "purple", "green"},
  });

  // One array arg.
  auto result = evaluate<SimpleVector<StringView>>(
      "concat_ws('--', c0)", makeRowVector({arrayVector}));
  auto expected1 = makeFlatVector<StringView>({
      "red--blue",
      "blue--yellow--orange",
      "",
      "",
      "red--purple--green",
  });
  velox::test::assertEqualVectors(expected1, result);

  // Two array args.
  result = evaluate<SimpleVector<StringView>>(
      "concat_ws('--', c0, c1)", makeRowVector({arrayVector, arrayVector}));
  auto expected2 = makeFlatVector<StringView>({
      "red--blue--red--blue",
      "blue--yellow--orange--blue--yellow--orange",
      "",
      "",
      "red--purple--green--red--purple--green",
  });
  velox::test::assertEqualVectors(expected2, result);
}

TEST_F(ConcatWsTest, mixedStringAndArrayArgs) {
  auto arrayVector = makeNullableArrayVector<StringView>({
      {"red", "blue"},
      {"blue", std::nullopt, "yellow", std::nullopt, "orange"},
      {},
      {std::nullopt},
      {"red", "purple", "green"},
      {""},
      {"", "green"},
  });

  auto result = evaluate<SimpleVector<StringView>>(
      "concat_ws('--', c0, 'foo', c1, 'bar', 'end', '')",
      makeRowVector({arrayVector, arrayVector}));
  // Empty string & its neighboring inputs are also separated with separator.
  auto expected = makeFlatVector<StringView>({
      "red--blue--foo--red--blue--bar--end--",
      "blue--yellow--orange--foo--blue--yellow--orange--bar--end--",
      "foo--bar--end--",
      "foo--bar--end--",
      "red--purple--green--foo--red--purple--green--bar--end--",
      "--foo----bar--end--",
      "--green--foo----green--bar--end--",
  });
  velox::test::assertEqualVectors(expected, result);
}

TEST_F(ConcatWsTest, nonconstantSeparator) {
  auto separatorVector =
      makeFlatVector<StringView>({"##", "--", "~~", "**", "++"});
  auto arrayVector = makeNullableArrayVector<StringView>({
      {"red", "blue"},
      {"blue", std::nullopt, "yellow", std::nullopt, "orange"},
      {"red", "blue"},
      {"blue", std::nullopt, "yellow", std::nullopt, "orange"},
      {"red", "purple", "green"},
  });

  auto result = evaluate<SimpleVector<StringView>>(
      "concat_ws(c0, c1, '|')", makeRowVector({separatorVector, arrayVector}));
  auto expected = makeFlatVector<StringView>({
      "red##blue##|",
      "blue--yellow--orange--|",
      "red~~blue~~|",
      "blue**yellow**orange**|",
      "red++purple++green++|",
  });
  velox::test::assertEqualVectors(expected, result);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
