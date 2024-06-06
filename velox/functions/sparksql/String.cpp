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

class ConcatWs : public exec::VectorFunction {
 public:
  explicit ConcatWs(const std::optional<std::string>& separator)
      : separator_(separator) {}

  // Calculate the total number of bytes in the result.
  size_t calculateTotalResultBytes(
      const SelectivityVector& rows,
      exec::EvalCtx& context,
      std::vector<exec::LocalDecodedVector>& decodedArrays,
      const std::vector<std::string>& constantStrings,
      const std::vector<exec::LocalDecodedVector>& decodedStringArgs,
      const exec::LocalDecodedVector& decodedSeparator) const {
    size_t totalResultBytes = 0;
    rows.applyToSelected([&](auto row) {
      int32_t allElements = 0;
      // Calculate size for array args.
      for (int i = 0; i < decodedArrays.size(); i++) {
        DecodedVector& arrayDecoded = *decodedArrays[i].get();
        auto baseArray = arrayDecoded.base()->as<ArrayVector>();
        auto rawSizes = baseArray->rawSizes();
        auto rawOffsets = baseArray->rawOffsets();
        auto indices = arrayDecoded.indices();
        auto elements = baseArray->elements();
        exec::LocalSelectivityVector nestedRows(context, elements->size());
        nestedRows.get()->setAll();
        exec::LocalDecodedVector elementsHolder(
            context, *elements, *nestedRows.get());
        auto& elementsDecoded = *elementsHolder.get();

        auto size = rawSizes[indices[row]];
        auto offset = rawOffsets[indices[row]];
        for (int j = 0; j < size; ++j) {
          if (!elementsDecoded.isNullAt(offset + j)) {
            auto element = elementsDecoded.valueAt<StringView>(offset + j);
            // No matter empty string or not.
            allElements++;
            totalResultBytes += element.size();
          }
        }
      }

      // Calculate size for string arg.
      auto it = decodedStringArgs.begin();
      for (int i = 0; i < constantStrings.size(); i++) {
        StringView value;
        if (!constantStrings[i].empty()) {
          value = StringView(constantStrings[i]);
        } else {
          // Skip NULL.
          if ((*it)->isNullAt(row)) {
            ++it;
            continue;
          }
          value = (*it++)->valueAt<StringView>(row);
        }
        // No matter empty string or not.
        allElements++;
        totalResultBytes += value.size();
      }

      int32_t separatorSize = separator_.has_value()
          ? separator_.value().size()
          : decodedSeparator->valueAt<StringView>(row).size();

      if (allElements > 1 && separatorSize > 0) {
        totalResultBytes += (allElements - 1) * separatorSize;
      }
    });
    return totalResultBytes;
  }

  void initVectors(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      const exec::EvalCtx& context,
      std::vector<exec::LocalDecodedVector>& decodedArrays,
      std::vector<column_index_t>& argMapping,
      std::vector<std::string>& constantStrings,
      std::vector<exec::LocalDecodedVector>& decodedStringArgs) const {
    for (auto i = 1; i < args.size(); ++i) {
      if (args[i] && args[i]->typeKind() == TypeKind::ARRAY) {
        decodedArrays.emplace_back(context, *args[i], rows);
        continue;
      }
      // Handles string arg.
      argMapping.push_back(i);
      if (!separator_.has_value()) {
        // Cannot concat consecutive constant string args in advance.
        constantStrings.push_back("");
        continue;
      }
      if (args[i] && args[i]->as<ConstantVector<StringView>>() &&
          !args[i]->as<ConstantVector<StringView>>()->isNullAt(0)) {
        std::ostringstream out;
        out << args[i]->as<ConstantVector<StringView>>()->valueAt(0);
        column_index_t j = i + 1;
        // Concat constant string args in advance.
        for (; j < args.size(); ++j) {
          if (!args[j] || args[j]->typeKind() == TypeKind::ARRAY ||
              !args[j]->as<ConstantVector<StringView>>() ||
              args[j]->as<ConstantVector<StringView>>()->isNullAt(0)) {
            break;
          }
          out << separator_.value()
              << args[j]->as<ConstantVector<StringView>>()->valueAt(0);
        }
        constantStrings.emplace_back(out.str());
        i = j - 1;
      } else {
        constantStrings.push_back("");
      }
    }

    // Number of string columns after combined consecutive constant ones.
    auto numStringCols = constantStrings.size();
    for (auto i = 0; i < numStringCols; ++i) {
      if (constantStrings[i].empty()) {
        auto index = argMapping[i];
        decodedStringArgs.emplace_back(context, *args[index], rows);
      }
    }
  }

  // ConcatWs implementation. It concatenates the arguments with the separator.
  // Mixed using of VARCHAR & ARRAY<VARCHAR> is considered. If separator is
  // constant, concatenate consecutive constant string args in advance. Then,
  // concatenate the intemediate result with neighboring array args or
  // non-constant string args.
  void doApply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      exec::EvalCtx& context,
      FlatVector<StringView>& flatResult) const {
    std::vector<column_index_t> argMapping;
    std::vector<std::string> constantStrings;
    auto numArgs = args.size();
    argMapping.reserve(numArgs - 1);
    // Save intermediate result for consecutive constant string args.
    // They will be concatenated in advance.
    constantStrings.reserve(numArgs - 1);
    std::vector<exec::LocalDecodedVector> decodedArrays;
    decodedArrays.reserve(numArgs - 1);
    // For column string arg decoding.
    std::vector<exec::LocalDecodedVector> decodedStringArgs;
    decodedStringArgs.reserve(numArgs);

    initVectors(
        rows,
        args,
        context,
        decodedArrays,
        argMapping,
        constantStrings,
        decodedStringArgs);
    exec::LocalDecodedVector decodedSeparator(context);
    if (!separator_.has_value()) {
      decodedSeparator = exec::LocalDecodedVector(context, *args[0], rows);
    }

    auto totalResultBytes = calculateTotalResultBytes(
        rows,
        context,
        decodedArrays,
        constantStrings,
        decodedStringArgs,
        decodedSeparator);

    // Allocate a string buffer.
    auto rawBuffer =
        flatResult.getRawStringBufferWithSpace(totalResultBytes, true);
    rows.applyToSelected([&](auto row) {
      const char* start = rawBuffer;
      auto isFirst = true;
      // For array arg.
      int32_t i = 0;
      // For string arg.
      int32_t j = 0;
      auto it = decodedStringArgs.begin();

      auto copyToBuffer = [&](StringView value, StringView separator) {
        if (isFirst) {
          isFirst = false;
        } else {
          // Add separator before the current value.
          if (!separator.empty()) {
            memcpy(rawBuffer, separator.data(), separator.size());
            rawBuffer += separator.size();
          }
        }
        if (!value.empty()) {
          memcpy(rawBuffer, value.data(), value.size());
          rawBuffer += value.size();
        }
      };

      for (auto itArgs = args.begin() + 1; itArgs != args.end(); ++itArgs) {
        if ((*itArgs)->typeKind() == TypeKind::ARRAY) {
          DecodedVector& arrayDecoded = *decodedArrays[i].get();
          auto baseArray = arrayDecoded.base()->as<ArrayVector>();
          auto rawSizes = baseArray->rawSizes();
          auto rawOffsets = baseArray->rawOffsets();
          auto indices = arrayDecoded.indices();
          auto elements = baseArray->elements();
          exec::LocalSelectivityVector nestedRows(context, elements->size());
          nestedRows.get()->setAll();
          exec::LocalDecodedVector elementsHolder(
              context, *elements, *nestedRows.get());
          auto& elementsDecoded = *elementsHolder.get();

          auto size = rawSizes[indices[row]];
          auto offset = rawOffsets[indices[row]];
          for (int k = 0; k < size; ++k) {
            if (!elementsDecoded.isNullAt(offset + k)) {
              auto element = elementsDecoded.valueAt<StringView>(offset + k);
              copyToBuffer(
                  element,
                  separator_.has_value()
                      ? StringView(separator_.value())
                      : decodedSeparator->valueAt<StringView>(row));
            }
          }
          i++;
          continue;
        }

        if (j >= constantStrings.size()) {
          continue;
        }

        StringView value;
        if (!constantStrings[j].empty()) {
          value = StringView(constantStrings[j]);
        } else {
          // Skip NULL.
          if ((*it)->isNullAt(row)) {
            ++it;
            continue;
          }
          value = (*it++)->valueAt<StringView>(row);
        }
        copyToBuffer(
            value,
            separator_.has_value()
                ? StringView(separator_.value())
                : decodedSeparator->valueAt<StringView>(row));
        j++;
      }
      flatResult.setNoCopy(row, StringView(start, rawBuffer - start));
    });
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    context.ensureWritable(rows, VARCHAR(), result);
    auto flatResult = result->asFlatVector<StringView>();
    auto numArgs = args.size();
    // If separator is NULL, result is NULL.
    if (args[0]->isNullAt(0)) {
      rows.applyToSelected([&](auto row) { result->setNull(row, true); });
      return;
    }
    // If only separator (not a NULL) is provided, result is an empty string.
    if (numArgs == 1) {
      rows.applyToSelected(
          [&](auto row) { flatResult->setNoCopy(row, StringView("")); });
      return;
    }
    doApply(rows, args, context, *flatResult);
  }

 private:
  // For holding constant separator.
  const std::optional<std::string> separator_;
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
          // Use "any" to allow the mixed using of these two types. The
          // argument type will be checked in makeConcatWs.
          //
          // varchar, [varchar], [array(varchar)], ... -> varchar.
          exec::FunctionSignatureBuilder()
              .returnType("varchar")
              .argumentType("varchar")
              .argumentType("any")
              .variableArity()
              .build()};
}

std::shared_ptr<exec::VectorFunction> makeConcatWs(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  auto numArgs = inputArgs.size();
  VELOX_USER_CHECK_GE(
      numArgs,
      1,
      "concat_ws requires one arguments at least, but got {}.",
      numArgs);
  for (const auto& arg : inputArgs) {
    VELOX_USER_CHECK(
        arg.type->isVarchar() ||
            (arg.type->isArray() &&
             arg.type->asArray().elementType()->isVarchar()),
        "concat_ws requires varchar or array(varchar) arguments, but got {}.",
        arg.type->toString());
  }

  BaseVector* constantSeparator = inputArgs[0].constantValue.get();
  std::optional<std::string> separator = std::nullopt;
  if (constantSeparator != nullptr) {
    separator =
        constantSeparator->as<ConstantVector<StringView>>()->valueAt(0).str();
  }

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
