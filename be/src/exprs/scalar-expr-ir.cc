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

#include "exprs/scalar-expr.h"
#include "udf/udf.h"

#ifdef IR_COMPILE

/// Compile ScalarExprEvaluator declaration to IR so we can use it in codegen'd functions
#include "exprs/scalar-expr-evaluator.h"

/// Dummy function to force compilation of UDF types.
/// The arguments are pointers to prevent Clang from lowering the struct types
/// (e.g. IntVal={bool, i32} can be coerced to i64).
void dummy(impala_udf::FunctionContext*, impala_udf::BooleanVal*, impala_udf::TinyIntVal*,
    impala_udf::SmallIntVal*, impala_udf::IntVal*, impala_udf::BigIntVal*,
    impala_udf::FloatVal*, impala_udf::DoubleVal*, impala_udf::StringVal*,
    impala_udf::TimestampVal*, impala_udf::DecimalVal*, impala::ScalarExprEvaluator*) { }
#endif

/// The following are compute functions that are cross-compiled to both native and IR
/// libraries. In the interpreted path, these functions are executed as-is from the native
/// code. In the codegen'd path, we load the IR functions and replace the Get*Val() calls
/// with the appropriate child's codegen'd compute function.

using namespace impala;
using namespace impala_udf;

/// Static wrappers around Get*Val() functions. We'd like to be able to call these from
/// directly from native code as well as from generated IR functions.
BooleanVal ScalarExpr::GetBooleanVal(
    ScalarExpr* expr, ScalarExprEvaluator* evaluator, const TupleRow* row) {
  return expr->GetBooleanVal(evaluator, row);
}

TinyIntVal ScalarExpr::GetTinyIntVal(
    ScalarExpr* expr, ScalarExprEvaluator* evaluator, const TupleRow* row) {
  return expr->GetTinyIntVal(evaluator, row);
}

SmallIntVal ScalarExpr::GetSmallIntVal(
    ScalarExpr* expr, ScalarExprEvaluator* evaluator, const TupleRow* row) {
  return expr->GetSmallIntVal(evaluator, row);
}

IntVal ScalarExpr::GetIntVal(
    ScalarExpr* expr, ScalarExprEvaluator* evaluator, const TupleRow* row) {
  return expr->GetIntVal(evaluator, row);
}

BigIntVal ScalarExpr::GetBigIntVal(
    ScalarExpr* expr, ScalarExprEvaluator* evaluator, const TupleRow* row) {
  return expr->GetBigIntVal(evaluator, row);
}

FloatVal ScalarExpr::GetFloatVal(
    ScalarExpr* expr, ScalarExprEvaluator* evaluator, const TupleRow* row) {
  return expr->GetFloatVal(evaluator, row);
}

DoubleVal ScalarExpr::GetDoubleVal(
    ScalarExpr* expr, ScalarExprEvaluator* evaluator, const TupleRow* row) {
  return expr->GetDoubleVal(evaluator, row);
}

StringVal ScalarExpr::GetStringVal(
    ScalarExpr* expr, ScalarExprEvaluator* evaluator, const TupleRow* row) {
  return expr->GetStringVal(evaluator, row);
}

TimestampVal ScalarExpr::GetTimestampVal(
    ScalarExpr* expr, ScalarExprEvaluator* evaluator, const TupleRow* row) {
  return expr->GetTimestampVal(evaluator, row);
}

DecimalVal ScalarExpr::GetDecimalVal(
    ScalarExpr* expr, ScalarExprEvaluator* evaluator, const TupleRow* row) {
  return expr->GetDecimalVal(evaluator, row);
}
