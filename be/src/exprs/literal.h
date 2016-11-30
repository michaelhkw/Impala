// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#ifndef IMPALA_EXPRS_LITERAL_H_
#define IMPALA_EXPRS_LITERAL_H_

#include <string>
#include "exprs/scalar-expr.h"
#include "exprs/expr-value.h"
#include "runtime/string-value.h"
#include "udf/udf.h"

using namespace impala_udf;

namespace impala {

class ScalarExprEvaluator;
class TExprNode;

class Literal: public ScalarExpr {
 public:
  virtual bool IsLiteral() const override { return true; }
  virtual Status GetCodegendComputeFn(LlvmCodeGen* codegen, llvm::Function** fn) override;
  virtual std::string DebugString() const override;

  /// Test function that parses 'str' according to 'type'. The caller owns the returned
  /// Literal.
  static Literal* CreateLiteral(const ColumnType& type, const std::string& str);

 protected:
  friend class ScalarExpr;
  friend class ScalarExprEvaluator;

  Literal(const TExprNode& node);

  /// Test ctors
  Literal(ColumnType type, bool v);
  Literal(ColumnType type, int8_t v);
  Literal(ColumnType type, int16_t v);
  Literal(ColumnType type, int32_t v);
  Literal(ColumnType type, int64_t v);
  Literal(ColumnType type, float v);
  Literal(ColumnType type, double v);
  Literal(ColumnType type, const std::string& v);
  Literal(ColumnType type, const StringValue& v);
  Literal(ColumnType type, const TimestampValue& v);

  virtual BooleanVal GetBooleanVal(ScalarExprEvaluator*, const TupleRow*) const override;
  virtual TinyIntVal GetTinyIntVal(ScalarExprEvaluator*, const TupleRow*) const override;
  virtual SmallIntVal GetSmallIntVal(
      ScalarExprEvaluator*, const TupleRow*) const override;
  virtual IntVal GetIntVal(ScalarExprEvaluator*, const TupleRow*) const override;
  virtual BigIntVal GetBigIntVal(ScalarExprEvaluator*, const TupleRow*) const override;
  virtual FloatVal GetFloatVal(ScalarExprEvaluator*, const TupleRow*) const override;
  virtual DoubleVal GetDoubleVal(ScalarExprEvaluator*, const TupleRow*) const override;
  virtual StringVal GetStringVal(ScalarExprEvaluator*, const TupleRow*) const override;
  virtual TimestampVal GetTimestampVal(
      ScalarExprEvaluator*, const TupleRow*) const override;
  virtual DecimalVal GetDecimalVal(ScalarExprEvaluator*, const TupleRow*) const override;

 private:
  ExprValue value_;
};

}

#endif
