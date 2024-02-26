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
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/StringEncodingUtils.h"
#include "velox/functions/lib/string/StringCore.h"

namespace facebook::velox::functions::sparksql {

using namespace stringCore;
namespace {

template <bool isAscii>
int32_t instr(
    const folly::StringPiece haystack,
    const folly::StringPiece needle) {
  int32_t offset = haystack.find(needle);
  if constexpr (isAscii) {
    return offset + 1;
  } else {
    // If the string is unicode, convert the byte offset to a codepoints.
    return offset == -1 ? 0 : lengthUnicode(haystack.data(), offset) + 1;
  }
}

class Instr : public exec::VectorFunction {
  bool ensureStringEncodingSetAtAllInputs() const override {
    return true;
  }

  void apply(
      const SelectivityVector& selected,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 2);
    VELOX_CHECK_EQ(args[0]->typeKind(), TypeKind::VARCHAR);
    VELOX_CHECK_EQ(args[1]->typeKind(), TypeKind::VARCHAR);
    exec::LocalDecodedVector haystack(context, *args[0], selected);
    exec::LocalDecodedVector needle(context, *args[1], selected);
    context.ensureWritable(selected, INTEGER(), result);
    auto* output = result->as<FlatVector<int32_t>>();

    if (isAscii(args[0].get(), selected)) {
      selected.applyToSelected([&](vector_size_t row) {
        auto h = haystack->valueAt<StringView>(row);
        auto n = needle->valueAt<StringView>(row);
        output->set(row, instr<true>(h, n));
      });
    } else {
      selected.applyToSelected([&](vector_size_t row) {
        auto h = haystack->valueAt<StringView>(row);
        auto n = needle->valueAt<StringView>(row);
        output->set(row, instr<false>(h, n));
      });
    }
  }
};

class Length : public exec::VectorFunction {
  bool ensureStringEncodingSetAtAllInputs() const override {
    return true;
  }

  void apply(
      const SelectivityVector& selected,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 1);
    VELOX_CHECK(
        args[0]->typeKind() == TypeKind::VARCHAR ||
        args[0]->typeKind() == TypeKind::VARBINARY);
    exec::LocalDecodedVector input(context, *args[0], selected);
    context.ensureWritable(selected, INTEGER(), result);
    auto* output = result->as<FlatVector<int32_t>>();

    if (args[0]->typeKind() == TypeKind::VARCHAR &&
        !isAscii(args[0].get(), selected)) {
      selected.applyToSelected([&](vector_size_t row) {
        const StringView str = input->valueAt<StringView>(row);
        output->set(row, lengthUnicode(str.data(), str.size()));
      });
    } else {
      selected.applyToSelected([&](vector_size_t row) {
        output->set(row, input->valueAt<StringView>(row).size());
      });
    }
  }
};

void doApply(
    const SelectivityVector& rows,
    std::vector<VectorPtr>& args,
    exec::EvalCtx& context,
    const std::string& separator,
    FlatVector<StringView>& flatResult) {
  std::vector<column_index_t> argMapping;
  std::vector<std::string> constantStrings;
  auto numArgs = args.size();
  // Save constant values to constantStrings_.
  // Identify and combine consecutive constant inputs.
  argMapping.reserve(numArgs - 1);
  constantStrings.reserve(numArgs - 1);
  // For each array arg, save rawSizes, rawOffsets, indices, and elements
  // BaseBector.
  std::vector<const vector_size_t*> rawSizesVector;
  std::vector<const vector_size_t*> rawOffsetsVector;
  std::vector<const vector_size_t*> indicesVector;
  std::vector<DecodedVector> decodedVectors;

  for (auto i = 1; i < numArgs; ++i) {
    if (args[i] && args[i]->typeKind() == TypeKind::ARRAY) {
      exec::LocalDecodedVector arrayHolder(context, *args[i], rows);
      auto& arrayDecoded = *arrayHolder.get();
      auto baseArray = arrayDecoded.base()->as<ArrayVector>();
      rawSizesVector.push_back(baseArray->rawSizes());
      rawOffsetsVector.push_back(baseArray->rawOffsets());
      indicesVector.push_back(arrayDecoded.indices());
      auto elements = baseArray->elements();
      exec::LocalSelectivityVector nestedRows(context, elements->size());
      nestedRows.get()->setAll();
      exec::LocalDecodedVector elementsHolder(
          context, *elements, *nestedRows.get());
      auto& elementsDecoded = *elementsHolder.get();
      decodedVectors.push_back(std::move(elementsDecoded));
      continue;
    }
    // Handles string arg.
    argMapping.push_back(i);
    if (args[i] && args[i]->as<ConstantVector<StringView>>() &&
        !args[i]->as<ConstantVector<StringView>>()->isNullAt(0)) {
      std::ostringstream out;
      out << args[i]->as<ConstantVector<StringView>>()->valueAt(0);
      column_index_t j = i + 1;
      // Concat constant string args.
      for (; j < numArgs; ++j) {
        if (!args[j] || args[j]->typeKind() == TypeKind::ARRAY ||
            !args[j]->as<ConstantVector<StringView>>() ||
            args[j]->as<ConstantVector<StringView>>()->isNullAt(0)) {
          break;
        }
        out << separator
            << args[j]->as<ConstantVector<StringView>>()->valueAt(0);
      }
      constantStrings.emplace_back(out.str());
      i = j - 1;
    } else {
      constantStrings.push_back("");
    }
  }

  // Number of string columns after combined constant ones.
  auto numStringCols = constantStrings.size();
  // For column string arg decoding.
  std::vector<exec::LocalDecodedVector> decodedStringArgs;
  decodedStringArgs.reserve(numStringCols);

  for (auto i = 0; i < numStringCols; ++i) {
    if (constantStrings[i].empty()) {
      auto index = argMapping[i];
      decodedStringArgs.emplace_back(context, *args[index], rows);
    }
  }

  // Calculate the total number of bytes in the result.
  size_t totalResultBytes = 0;
  rows.applyToSelected([&](auto row) {
    int32_t allElements = 0;
    for (int i = 0; i < rawSizesVector.size(); i++) {
      auto size = rawSizesVector[i][indicesVector[i][row]];
      auto offset = rawOffsetsVector[i][indicesVector[i][row]];
      for (int j = 0; j < size; ++j) {
        if (!decodedVectors[i].isNullAt(offset + j)) {
          auto element = decodedVectors[i].valueAt<StringView>(offset + j);
          if (!element.empty()) {
            allElements++;
            totalResultBytes += element.size();
          }
        }
      }
    }
    auto it = decodedStringArgs.begin();
    for (int i = 0; i < numStringCols; i++) {
      auto value = constantStrings[i].empty()
          ? (*it++)->valueAt<StringView>(row)
          : StringView(constantStrings[i]);
      if (!value.empty()) {
        allElements++;
        totalResultBytes += value.size();
      }
    }
    if (allElements > 1) {
      totalResultBytes += (allElements - 1) * separator.size();
    }
  });

  // Allocate a string buffer.
  auto rawBuffer = flatResult.getRawStringBufferWithSpace(totalResultBytes);
  size_t bufferOffset = 0;
  rows.applyToSelected([&](int row) {
    const char* start = rawBuffer + bufferOffset;
    size_t combinedSize = 0;
    auto isFirst = true;
    // For array arg.
    int32_t i = 0;
    // For string arg.
    int32_t j = 0;
    auto it = decodedStringArgs.begin();

    auto copyToBuffer = [&](StringView value) {
      if (value.empty()) {
        return;
      }
      if (isFirst) {
        isFirst = false;
      } else {
        // Add separator before the current value.
        memcpy(rawBuffer + bufferOffset, separator.data(), separator.size());
        bufferOffset += separator.size();
        combinedSize += separator.size();
      }
      memcpy(rawBuffer + bufferOffset, value.data(), value.size());
      combinedSize += value.size();
      bufferOffset += value.size();
    };

    for (auto itArgs = args.begin() + 1; itArgs != args.end(); ++itArgs) {
      if ((*itArgs)->typeKind() == TypeKind::ARRAY) {
        auto size = rawSizesVector[i][indicesVector[i][row]];
        auto offset = rawOffsetsVector[i][indicesVector[i][row]];
        for (int k = 0; k < size; ++k) {
          if (!decodedVectors[i].isNullAt(offset + k)) {
            auto element = decodedVectors[i].valueAt<StringView>(offset + k);
            copyToBuffer(element);
          }
        }
        i++;
        continue;
      }
      if (j >= numStringCols) {
        continue;
      }
      StringView value;
      if (constantStrings[j].empty()) {
        value = (*it++)->valueAt<StringView>(row);
      } else {
        value = StringView(constantStrings[j]);
      }
      copyToBuffer(value);
      j++;
    }
    flatResult.setNoCopy(row, StringView(start, combinedSize));
  });
}

class ConcatWs : public exec::VectorFunction {
 public:
  explicit ConcatWs(const std::string& separator) : separator_(separator) {}

  void apply(
      const SelectivityVector& selected,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    context.ensureWritable(selected, VARCHAR(), result);
    auto flatResult = result->asFlatVector<StringView>();
    auto numArgs = args.size();
    // If separator is NULL, result is NULL.
    if (args[0]->isNullAt(0)) {
      selected.applyToSelected([&](int row) { result->setNull(row, true); });
      return;
    }
    // If only separator (not a NULL) is provided, result is an empty string.
    if (numArgs == 1) {
      selected.applyToSelected(
          [&](int row) { flatResult->setNoCopy(row, StringView("")); });
      return;
    }
    doApply(selected, args, context, separator_, *flatResult);
  }

 private:
  const std::string separator_;
};

} // namespace

std::vector<std::shared_ptr<exec::FunctionSignature>> instrSignatures() {
  return {
      exec::FunctionSignatureBuilder()
          .returnType("INTEGER")
          .argumentType("VARCHAR")
          .argumentType("VARCHAR")
          .build(),
  };
}

std::shared_ptr<exec::VectorFunction> makeInstr(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  static const auto kInstrFunction = std::make_shared<Instr>();
  return kInstrFunction;
}

std::vector<std::shared_ptr<exec::FunctionSignature>> lengthSignatures() {
  return {
      exec::FunctionSignatureBuilder()
          .returnType("INTEGER")
          .argumentType("VARCHAR")
          .build(),
      exec::FunctionSignatureBuilder()
          .returnType("INTEGER")
          .argumentType("VARBINARY")
          .build(),
  };
}

std::shared_ptr<exec::VectorFunction> makeLength(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  static const auto kLengthFunction = std::make_shared<Length>();
  return kLengthFunction;
}

std::vector<std::shared_ptr<exec::FunctionSignature>> concatWsSignatures() {
  return {// The second and folowing arguments are varchar or array(varchar).
          // The argument type will be checked in makeConcatWs.
          // varchar, [varchar], [array(varchar)], ... -> varchar.
          exec::FunctionSignatureBuilder()
              .returnType("varchar")
              .constantArgumentType("varchar")
              .argumentType("any")
              .variableArity()
              .build()};
}

std::shared_ptr<exec::VectorFunction> makeConcatWs(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  auto numArgs = inputArgs.size();
  VELOX_USER_CHECK(
      numArgs >= 1,
      "concat_ws requires one arguments at least, but got {}.",
      numArgs);
  for (auto& arg : inputArgs) {
    VELOX_USER_CHECK(
        arg.type->isVarchar() ||
            (arg.type->isArray() &&
             arg.type->asArray().elementType()->isVarchar()),
        "concat_ws requires varchar or array(varchar) arguments, but got {}.",
        arg.type->toString());
  }

  BaseVector* constantPattern = inputArgs[0].constantValue.get();
  VELOX_USER_CHECK(
      nullptr != constantPattern,
      "concat_ws requires constant separator arguments.");

  auto separator =
      constantPattern->as<ConstantVector<StringView>>()->valueAt(0).str();
  return std::make_shared<ConcatWs>(separator);
}

void encodeDigestToBase16(uint8_t* output, int digestSize) {
  static unsigned char const kHexCodes[] = "0123456789abcdef";
  for (int i = digestSize - 1; i >= 0; --i) {
    int digestChar = output[i];
    output[i * 2] = kHexCodes[(digestChar >> 4) & 0xf];
    output[i * 2 + 1] = kHexCodes[digestChar & 0xf];
  }
}

} // namespace facebook::velox::functions::sparksql
