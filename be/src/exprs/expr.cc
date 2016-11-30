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

#include "exprs/expr.h"

#include <sstream>
#include <thrift/protocol/TDebugProtocol.h>

#include "common/object-pool.h"
#include "common/status.h"
#include "exprs/scalar-expr.h"
#include "runtime/lib-cache.h"
#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/ImpalaService_types.h"

#include "common/names.h"

namespace impala {

const char* Expr::LLVM_CLASS_NAME = "class.impala::Expr";

Expr::Expr(const ColumnType& type, bool is_constant)
    : cache_entry_(nullptr),
      is_constant_(is_constant),
      type_(type) {
}

Expr::Expr(const TExprNode& node)
    : cache_entry_(nullptr),
      is_constant_(node.is_constant),
      type_(ColumnType::FromThrift(node.type)) {
  if (node.__isset.fn) fn_ = node.fn;
}

Expr::~Expr() {
  DCHECK(cache_entry_ == nullptr);
}

Status Expr::CreateTree(
    ObjectPool* pool, const TExpr& texpr, Expr* root, int* fn_ctx_idx_ptr) {
  DCHECK(!texpr.nodes.empty());
  DCHECK(root != nullptr);

  int node_idx = 0;
  Status status =
      CreateTreeFromThrift(pool, texpr.nodes, root, &node_idx, fn_ctx_idx_ptr);
  if (UNLIKELY(!status.ok())) {
    LOG(ERROR) << "Could not construct expr tree.\n" << status.GetDetail() << "\n"
               << apache::thrift::ThriftDebugString(texpr);
    return status;
  }
  if (UNLIKELY(node_idx + 1 != texpr.nodes.size())) {
    return Status("Expression tree only partially reconstructed. Not all thrift " \
                  "nodes were used.");
  }
  return Status::OK();
}

Status Expr::CreateTreeFromThrift(ObjectPool* pool, const vector<TExprNode>& nodes,
    Expr* parent, int* node_idx, int* fn_ctx_idx_ptr) {
  DCHECK(fn_ctx_idx_ptr != nullptr);
  // propagate error case
  if (*node_idx >= nodes.size()) {
    return Status("Failed to reconstruct expression tree from thrift.");
  }

  // The root of the tree at nodes[0] is already created and stored in 'parent'.
  // Skip creating a new node in that case.
  ScalarExpr* child_expr = nullptr;
  if (*node_idx > 0) {
    const TExprNode& texpr_node = nodes[*node_idx];
    DCHECK_NE(texpr_node.node_type, TExprNodeType::AGGREGATE_EXPR);
    RETURN_IF_ERROR(
        ScalarExpr::CreateNode(pool, texpr_node, &child_expr, fn_ctx_idx_ptr));
    parent->AddChild(child_expr);
  }

  Expr* root = *node_idx == 0 ? parent : child_expr;
  DCHECK(root != nullptr);
  const int num_children = nodes[*node_idx].num_children;
  const bool isAggFn = root->IsAggFn();
  DCHECK(!isAggFn || *node_idx == 0);
  for (int i = 0; i < num_children; ++i) {
    // Each input expression for AggFn uses a separate evaluator.
    // Reset the function context index for each input expression.
    if (isAggFn) *fn_ctx_idx_ptr = 0;
    const int fn_ctx_idx_start = *fn_ctx_idx_ptr;
    *node_idx += 1;
    RETURN_IF_ERROR(CreateTreeFromThrift(pool, nodes, root, node_idx, fn_ctx_idx_ptr));
    ScalarExpr* child = root->GetChild(i);
    DCHECK(child != nullptr);
    child->fn_ctx_idx_start_ = fn_ctx_idx_start;
    child->fn_ctx_idx_end_ = *fn_ctx_idx_ptr;
  }
  return Status::OK();
}

void Expr::Close() {
  for (ScalarExpr* child : children_) child->Close();
  if (cache_entry_ != nullptr) {
    LibCache::instance()->DecrementUseCount(cache_entry_);
    cache_entry_ = nullptr;
  }
}

}
