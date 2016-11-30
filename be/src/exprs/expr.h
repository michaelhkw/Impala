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


/// --- Expr overview
///
/// Expr is a class which embodies expressions embedded in various operators in a query
/// plan fragment. For instance, it can represent a join expression in a PHJ node, a
/// predicate in a scan node or the aggregate function in a PAGG node. There are two
/// subclasses for Expr: ScalarExpr for scalar expressions and AggFn for aggregate
/// functions. A scalar expression computes a value over a single row while an aggregate
/// function computes a value over a set of rows.
///
/// Expr is internally represented as a tree of nodes. The root node can be either a
/// ScalarExpr or an AggFn node and all descendants are ScalarExpr nodes. Each Expr
/// subclass has its corresponding evaluator class. Expr contains static data about an
/// expression while the evaluator contains runtime states for evaluating Expr during
/// execution. An Expr can be shared by multiple evaluators.
///
/// Please see the headers of ScalarExpr and AggFn for details.
///

#ifndef IMPALA_EXPRS_EXPR_H
#define IMPALA_EXPRS_EXPR_H

#include <memory>
#include <string>
#include <vector>
#include <boost/scoped_ptr.hpp>

#include "common/global-types.h"
#include "common/status.h"
#include "impala-ir/impala-ir-functions.h"
#include "runtime/types.h"
#include "udf/udf-internal.h" // for CollectionVal
#include "udf/udf.h"

using namespace impala_udf;

namespace impala {

class IsNullExpr;
class LibCacheEntry;
class LlvmCodeGen;
class MemTracker;
class ObjectPool;
class RowDescriptor;
class RuntimeState;
class ScalarExpr;
class SlotDescriptor;
class TColumnValue;
class TExpr;
class TExprNode;
class Tuple;
class TupleRow;

/// This is the superclass of both ScalarExpr and AggFn.
class Expr {
 public:

  /// Creates and initializes an Expr tree from the thrift Expr 'texpr'.
  /// 'root' is an initialized Expr root node (i.e. either a ScalarExpr or AggFn)
  /// created from texpr.nodes[0]; 'fn_ctx_idx_ptr' is pointer to the index of the
  /// next available entry in the FunctionContext vector. This index is updated
  /// during the construction of the Expr tree.
  static Status CreateTree(
      ObjectPool* pool, const TExpr& texpr, Expr* root, int* fn_ctx_idx_ptr);

  const std::string& function_name() const { return fn_.name.function_name; }

  virtual ~Expr();

  /// Returns true if the given Expr is an AggFn. Overridden by AggFn.
  virtual bool IsAggFn() const { return false; }

  ScalarExpr* GetChild(int i) const { return children_[i]; }
  int GetNumChildren() const { return children_.size(); }

  const ColumnType& type() const { return type_; }
  bool is_constant() const { return is_constant_; }
  const std::vector<ScalarExpr*>& children() const { return children_; }

  /// Releases cache entries to LibCache in all nodes of the Expr tree.
  virtual void Close();

  /// Simple debug string that provides no expr subclass-specific information.
  /// Must be overriden by subclasses.
  virtual std::string DebugString() const = 0;

  static const char* LLVM_CLASS_NAME;

 protected:

  Expr(const ColumnType& type, bool is_constant);
  Expr(const TExprNode& node);

  /// Cache entry for the UDF or UDAF loaded from the library. Used by AggFn and
  /// some ScalarExpr such as ScalarFnCall. NULL if it's not used.
  LibCacheEntry* cache_entry_;

  /// The thrift function. Set only for AggFn and some ScalarExpr such as ScalarFnCall.
  TFunction fn_;

  /// True if this expr should be treated as a constant expression. True if either:
  /// * This expr was sent from the frontend and Expr.isConstant() was true.
  /// * This expr is a constant literal created in the backend.
  const bool is_constant_;

  /// Return type of the expression.
  const ColumnType type_;

  /// Sub-expressions of this expression tree.
  std::vector<ScalarExpr*> children_;

 private:
  friend class ExprTest;
  friend class ExprCodegenTest;

  /// Add a child expression to this Expr node.
  void AddChild(ScalarExpr* expr) { children_.push_back(expr); }

  /// Creates an expr tree with root 'parent' via depth-first traversal.
  /// Called recursively to create expr tree of sub-expression.
  /// parameters
  ///   pool: Object pool in which Expr created from nodes are stored
  ///   nodes: vector of thrift expression nodes to be translated
  ///   parent: parent of the new subtree to be created
  ///   node_idx: index in 'nodes' of the root of the next child TExprNode tree
  ///             to be converted and added to 'parent'. Updated as 'nodes' are
  ///             consumed to construct the tree
  ///   fn_ctx_idx_ptr: pointer to the next available index in FunctionContext vector
  /// return
  ///   status.ok() if successful
  ///   !status.ok() if tree is inconsistent or corrupt
  static Status CreateTreeFromThrift(ObjectPool* pool,
      const std::vector<TExprNode>& nodes, Expr* parent, int* node_idx,
      int* fn_ctx_idx_ptr);
};

}

#endif
