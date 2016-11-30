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

#ifndef IMPALA_EXPRS_EXPR_CONTEXT_H
#define IMPALA_EXPRS_EXPR_CONTEXT_H

#include <boost/scoped_ptr.hpp>

#include "common/object-pool.h"
#include "common/status.h"
#include "exprs/expr-value.h"
#include "udf/udf-internal.h" // for CollectionVal
#include "udf/udf.h"

using namespace impala_udf;

namespace impala {

class Expr;
class MemPool;
class MemTracker;
class RuntimeState;
class RowDescriptor;
class TColumnValue;
class TupleRow;

/// An ExprContext contains thread private states for the evaluation of an Expr Tree.
/// It holds the evaluation result which allows multiple threads to evaluate the same
/// expression on different rows concurrently using multiple ExprContexts. It also holds
/// FunctionContexts needed by any sub-expression in the Expr tree. A single ExprContext
/// is not thread-safe. Please see the class comment in expr.h for relationship with Expr
/// and FunctionContext.
///
/// TODO: rename ExprContext to ExprEvaluator.

class ExprContext {
 public:
  ~ExprContext();

  /// Allocates all FunctionContexts for this ExprContext. Also initialize the expr tree
  /// for evaluation. Allocations from this context will be counted against 'tracker'.
  /// If Prepare() is called, Close() must be called before destruction to release
  /// resources, regardless of whether Prepare() succeeded.
  Status Prepare(RuntimeState* state, const RowDescriptor& row_desc, MemTracker* tracker);

  /// Initializes the FunctionContexts in the ExprContext on all nodes in the Expr tree.
  /// Must be called after calling Prepare(). Does not need to be called on clones.
  /// Idempotent (this allows exprs to be opened multiple times in subplans without
  /// reinitializing function states).
  Status Open(RuntimeState* state);

  /// Creates a copy of this ExprContext. Open() must be called first. The copy contains
  /// clones of each FunctionContext, which share the fragment-local state of the
  /// originals but have their own MemPool and thread-local state. Clone() should be used
  /// to create an ExprContext for each execution thread that needs to evaluate
  /// 'root'. Note that clones are already opened. '*new_context' must be initialized by
  /// the caller to NULL. The cloned ExprContext cannot be used after the original
  /// ExprContext is destroyed because it may reference fragment-local state from the
  /// original.
  Status Clone(RuntimeState* state, ExprContext** new_context);

  /// Clones each ExprContext for multiple expr trees. 'new_ctxs' must be non-NULL.
  /// Idempotent: if '*new_ctxs' is empty, a clone of each context in 'ctxs' will be added
  /// to it, and if non-empty, it is assumed CloneIfNotExists() was already called and the
  /// call is a no-op. The new ExprContexts are created in state->obj_pool().
  static Status CloneIfNotExists(const std::vector<ExprContext*>& ctxs,
      RuntimeState* state, std::vector<ExprContext*>* new_ctxs);

  /// Closes all FunctionContexts. Must be called on every ExprContext, including clones.
  /// Has no effect if already closed.
  void Close(RuntimeState* state);

  /// Returns a newly created ExprContext object for 'expr' and adds it to 'pool'.
  /// Use this interface except for creating ExprContext on a stack.
  static ExprContext* Create(ObjectPool* pool, Expr* expr);

  /// Creates ExprContexts for all expressions in 'exprs' and returns them in
  /// 'expr_ctxs'. All ExprContexts are stored in 'pool'.
  static void Create(ObjectPool* pool, const std::vector<Expr*>& exprs,
      std::vector<ExprContext*>* expr_ctxs);

  /// Convenience function for preparing multiple ExprContexts.
  /// Allocations from 'ctxs' will be counted against 'tracker'.
  static Status Prepare(const std::vector<ExprContext*>& ctxs, RuntimeState* state,
      const RowDescriptor& row_desc, MemTracker* tracker);

  /// Convenience function for opening multiple ExprContexts.
  static Status Open(const std::vector<ExprContext*>& ctxs, RuntimeState* state);

  /// Convenience function for closing multiple ExprContexts.
  static void Close(const std::vector<ExprContext*>& ctxs, RuntimeState* state);

  static std::string DebugString(const std::vector<ExprContext*>& ctxs);

  /// Calls the appropriate Get*Val() function on this context's expr tree and stores the
  /// result in result_.
  void* GetValue(const TupleRow* row);

  /// Convenience function for evaluating Exprs that don't reference slots from the FE.
  /// Extracts value into 'col_val' and sets the appropriate __isset flag. No fields are
  /// set for NULL values. The specific field in 'col_val' that receives the value is
  /// based on the expr type:
  /// TYPE_BOOLEAN: boolVal
  /// TYPE_TINYINT/SMALLINT/INT: intVal
  /// TYPE_BIGINT: longVal
  /// TYPE_FLOAT/DOUBLE: doubleVal
  /// TYPE_STRING: binaryVal. Do not populate stringVal directly because BE/FE
  ///              conversions do not work properly for strings with ASCII chars
  ///              above 127. Pass the raw bytes so the caller can decide what to
  ///              do with the result (e.g., bail constant folding).
  /// TYPE_TIMESTAMP: binaryVal has the raw data, stringVal its string representation.
  void EvaluateWithoutRow(TColumnValue* col_val);

  /// Returns an error status if FunctionContexts associated with this Expr tree
  /// have any error. ['start', 'end') is the range inside the vector of FunctionContext
  /// in this ExprContext object. The default value of -1 for 'end' means the end of the
  /// vector of FunctionContext.
  Status GetFnContextError(int start = 0, int end = -1);

  /// Convenience functions: print value into 'str' or 'stream'.  NULL turns into "NULL".
  void PrintValue(const TupleRow* row, std::string* str);
  void PrintValue(void* value, std::string* str);
  void PrintValue(void* value, std::stringstream* stream);
  void PrintValue(const TupleRow* row, std::stringstream* stream);

  /// Retrieves a registered FunctionContext. 'i' is the 'fn_context_index_' of the
  /// corresponding Expr in the Expr tree.
  FunctionContext* fn_context(int i) {
    DCHECK_GE(i, 0);
    DCHECK_LT(i, fn_contexts_.size());
    return fn_contexts_[i];
  }

  /// Get the number of digits after the decimal that should be displayed for this value.
  /// Returns -1 if no scale has been specified (currently the scale is only set for
  /// doubles set by RoundUpTo). GetValue() must have already been called.
  /// TODO: remove this (IMPALA-4720).
  int output_scale() const { return output_scale_; }

  Expr* root() const { return root_; }
  bool opened() const { return opened_; }
  bool closed() const { return closed_; }
  bool is_clone() const { return is_clone_; }

  /// Calls Get*Val on root_
  BooleanVal GetBooleanVal(TupleRow* row);
  TinyIntVal GetTinyIntVal(TupleRow* row);
  SmallIntVal GetSmallIntVal(TupleRow* row);
  IntVal GetIntVal(TupleRow* row);
  BigIntVal GetBigIntVal(TupleRow* row);
  FloatVal GetFloatVal(TupleRow* row);
  DoubleVal GetDoubleVal(TupleRow* row);
  StringVal GetStringVal(TupleRow* row);
  CollectionVal GetCollectionVal(TupleRow* row);
  TimestampVal GetTimestampVal(TupleRow* row);
  DecimalVal GetDecimalVal(TupleRow* row);

  /// Returns true if any of the expression contexts in the array has local allocations.
  /// The last two are helper functions.
  static bool HasLocalAllocations(const std::vector<ExprContext*>& ctxs);
  bool HasLocalAllocations();
  static bool HasLocalAllocations(const std::vector<FunctionContext*>& fn_ctxs);

  /// Frees all local allocations made by fn_contexts_. This can be called when result
  /// data from this context is no longer needed. The last two are helper functions.
  static void FreeLocalAllocations(const std::vector<ExprContext*>& ctxs);
  void FreeLocalAllocations();
  static void FreeLocalAllocations(const std::vector<FunctionContext*>& ctxs);

  static const char* LLVM_CLASS_NAME;

 private:
  friend class Expr;
  /// Users of private GetValue() or 'pool_'.
  friend class CaseExpr;
  friend class HiveUdfCall;
  friend class ScalarFnCall;

  /// FunctionContexts for each registered expression. The FunctionContexts are created
  /// and owned by this ExprContext.
  std::vector<FunctionContext*> fn_contexts_;

  /// Array access to fn_contexts_. Used by ScalarFnCall's codegen'd compute function
  /// to access the correct FunctionContext.
  /// TODO: revisit this
  FunctionContext** fn_contexts_ptr_;

  /// Pool backing fn_contexts_. Counts against the runtime state's UDF mem tracker.
  boost::scoped_ptr<MemPool> pool_;

  /// The expr tree this context is for.
  Expr* root_;

  /// Stores the result of the root expr. This is used in interpreted code when we need a
  /// void*.
  ExprValue result_;

  /// True if this context came from a Clone() call. Used to manage FunctionStateScope.
  bool is_clone_;

  /// Variables keeping track of current state.
  bool prepared_;
  bool opened_;
  bool closed_;

  ExprContext(Expr* root);

  /// The number of digits after the decimal that should be displayed for this value.
  /// -1 if no scale has been specified (currently the scale is only set for doubles
  /// set by RoundUpTo).
  int output_scale_;

  /// Calls the appropriate Get*Val() function on 'e' and stores the result in result_.
  /// This is used by Exprs to call GetValue() on a child expr, rather than root_.
  void* GetValue(Expr* e, const TupleRow* row);

  /// Walks the expression tree 'expr' and fills in 'fn_contexts_' for all Expr nodes
  /// which need FunctionContext.
  void CreateFnContexts(RuntimeState* state, Expr* expr);
};

}

#endif
