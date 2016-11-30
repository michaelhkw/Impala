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


/// --- Terminology:
//
/// Compute function: The function that, given a row, performs the computation of an expr
/// and produces a scalar result. This function evaluates the necessary child arguments by
/// calling their compute functions, then performs whatever computation is necessary on
/// the arguments (e.g. calling a UDF with the child arguments). All compute functions
/// take arguments (ExprContext*, const TupleRow*). The return type is a *Val (i.e. a subclass
/// of AnyVal). Thus, a single expression will implement a compute function for every
/// return type it supports.
///
/// UDX: user-defined X. E.g., user-defined function, user-defined aggregate. Something
/// that is written by an external user.
///
/// Scalar function call: An expr that returns a single scalar value and can be
/// implemented using the UDF interface. Note that this includes builtins, which although
/// not being user-defined still use the same interface as UDFs (i.e., they are
/// implemented as functions with signature "*Val (FunctionContext*, *Val, *Val...)").
///
/// Aggregate function call: a UDA or builtin aggregate function.
///
/// --- Expr overview:
///
/// The Expr superclass defines a virtual Get*Val() compute function for each possible
/// return type (GetBooleanVal(), GetStringVal(), etc). Expr subclasses implement the
/// Get*Val() functions associated with their possible return types; for many Exprs this
/// will be a single function. These functions are generally cross-compiled to both native
/// and IR libraries. In the interpreted path, the native compute functions are run as-is.
///
/// For the codegen path, Expr defines a virtual method GetCodegendComputeFn() that
/// returns the Function* of the expr's compute function. Note that we do not need a
/// separate GetCodegendComputeFn() for each type.
///
/// Only short-circuited operators (e.g. &&, ||) and other special functions like literals
/// must implement custom Get*Val() compute functions. Scalar function calls use the
/// generic compute functions implemented by ScalarFnCall(). For cross-compiled compute
/// functions, GetCodegendComputeFn() can use ReplaceChildCallsComputeFn(), which takes a
/// cross-compiled IR Get*Val() function, pulls out any calls to the children's Get*Val()
/// functions (which we identify via the Get*Val() static wrappers), and replaces them
/// with the codegen'd version of that function. This allows us to write a single function
/// for both the interpreted and codegen paths.
///
/// Only short-circuited operators (e.g. &&, ||) and other special functions like
/// literals must implement custom Get*Val() compute functions. Scalar function calls
/// use the generic compute functions implemented by ScalarFnCall(). For cross-compiled
/// compute functions, GetCodegendComputeFn() can use ReplaceChildCallsComputeFn(), which
/// takes a cross-compiled IR Get*Val() function, pulls out any calls to the children's
/// Get*Val() functions (which we identify via the Get*Val() static wrappers), and
/// replaces them with the codegen'd version of that function. This allows us to write a
/// single function for both the interpreted and codegen paths.
///
/// --- Expr users (e.g. exec nodes):
///
/// A typical usage pattern will look something like:
/// 1. Expr::CreateExprTrees()
/// 2. ExprContext::Prepare()
/// 3. ExprContext::Open()
/// 4. ExprContext::CloneIfNotExists() [for multi-threaded execution]
/// 5. Evaluate exprs via Get*Val() calls
/// 6. ExprContext::Close() [called once per ExprContext, including clones]
///
/// Expr users should use the static Get*Val() wrapper functions to evaluate exprs,
/// cross-compile the resulting function, and use ReplaceGetValCalls() to create the
/// codegen'd function. See the comments on these functions for more details. This is a
/// similar pattern to that used by the cross-compiled compute functions.
///
/// --- Relationship with ExprContext and FunctionContext
///
/// Expr is shared by multiple threads / multiple fragment instances. An expression in
/// a query is represented as a tree of Expr whose states are static after initialization
/// by their Prepare() functions.
///
/// ExprContext contains thread private states (e.g. evaluation result, FunctionContext).
/// This separation allows multiple threads to evaluate the same Expr on different rows
/// in parallel. An ExprContext object references the root of its corresponding Expr tree
/// via the field 'root_'.
///
/// FunctionContext is the interface of UDF and UDA to the rest of Impala. It is passed
/// to UDF/UDA for them to store thread-private states specific to the UDF/UDA, propagates
/// errors and allocates memory. Within each expression, there can be multiple
/// sub-expressions which require FunctionContexts.
///
/// Expressions in exec nodes are expected to be initialized once per fragment and the
/// expressions' static states are shared among all instances of a fragment. Exec nodes in
/// each fragment instance will allocate their own ExprContexts and associate them with
/// the shared Expr object. FunctionContexts are stored in a vector inside ExprContext. An
/// sub-expression (i.e. a sub-tree of the Expr tree) may own a range inside this vector
/// whose start and end are indicated by 'fn_context_start_' and 'fn_context_end_'
/// respectively. 'fn_context_index_' is this Expr's index into the FunctionContexts
/// vector. This Expr can itself be a sub-expression in another expression. It's -1 if no
/// FunctionContext is needed.

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

namespace llvm {
  class BasicBlock;
  class Function;
  class Type;
  class Value;
};

namespace impala {

class ExprContext;
class IsNullExpr;
class LibCacheEntry;
class LlvmCodeGen;
class MemTracker;
class ObjectPool;
class RowDescriptor;
class RuntimeState;
class TColumnValue;
class TExpr;
class TExprNode;
class Tuple;
class TupleRow;

/// This is the superclass of all expr evaluation nodes.
class Expr {
 public:
  virtual ~Expr();

  /// Virtual compute functions for each *Val type. Each Expr subclass should implement
  /// the functions for the return type(s) it supports. For example, a boolean function
  /// will only implement GetBooleanVal(). Some Exprs, like Literal, have many possible
  /// return types and will implement multiple Get*Val() functions.
  virtual BooleanVal GetBooleanVal(ExprContext* context, const TupleRow*);
  virtual TinyIntVal GetTinyIntVal(ExprContext* context, const TupleRow*);
  virtual SmallIntVal GetSmallIntVal(ExprContext* context, const TupleRow*);
  virtual IntVal GetIntVal(ExprContext* context, const TupleRow*);
  virtual BigIntVal GetBigIntVal(ExprContext* context, const TupleRow*);
  virtual FloatVal GetFloatVal(ExprContext* context, const TupleRow*);
  virtual DoubleVal GetDoubleVal(ExprContext* context, const TupleRow*);
  virtual StringVal GetStringVal(ExprContext* context, const TupleRow*);
  virtual CollectionVal GetCollectionVal(ExprContext* context, const TupleRow*);
  virtual TimestampVal GetTimestampVal(ExprContext* context, const TupleRow*);
  virtual DecimalVal GetDecimalVal(ExprContext* context, const TupleRow*);

  inline void AddChild(Expr* expr) { children_.push_back(expr); }
  inline Expr* GetChild(int i) const { return children_[i]; }
  inline int GetNumChildren() const { return children_.size(); }

  inline const ColumnType& type() const { return type_; }
  inline bool is_slotref() const { return is_slotref_; }

  inline const std::vector<Expr*>& children() const { return children_; }

  inline int fn_context_index() const { return fn_context_index_; }
  inline int NumFnContexts() const {
    return fn_context_index_end_ - fn_context_index_start_;
  }

  /// Returns true if the expression is considered constant. This must match the
  /// definition of Expr.isConstant() in the frontend. The default implementation returns
  /// true if all children are constant.
  /// TODO: IMPALA-4617 - plumb through the value from the frontend and remove duplicate
  /// logic.
  virtual bool IsConstant() const;

  /// Returns true if this is a literal expression.
  virtual bool IsLiteral() const;

  /// Returns the number of SlotRef nodes in the expr tree. If this returns 0, it means it
  /// is valid to call GetValue(nullptr) on the expr tree.
  /// If 'slot_ids' is non-null, add the slot ids to it.
  virtual int GetSlotIds(std::vector<SlotId>* slot_ids = nullptr) const;

  /// Returns true iff the expression 'texpr' contains UDF available only as LLVM IR. In
  /// which case, it's impossible to interpret this expression and codegen must be used.
  static bool NeedCodegen(const TExpr& texpr);

  /// Create expression tree from the list of nodes contained in texpr within 'pool'.
  /// Returns the root of the expression in 'expr'.
  static Status CreateExprTree(ObjectPool* pool, const TExpr& texpr, Expr** expr);

  /// Creates vector of Exprs from the given vector of TExprs within 'pool' and returns
  /// them in 'exprs'. Returns an error if any ExprTree causes error, otherwise OK.
  static Status CreateExprTrees(ObjectPool* pool, const std::vector<TExpr>& texprs,
      std::vector<Expr*>* exprs);

  /// Creates vector of input Exprs of a function call expression 'texpr' Returns error
  /// if there is any error in creating the ExprContexts for the input expressions.
  /// Returns OK otherwise.
  static Status CreateInputExprTrees(ObjectPool* pool, const TExpr& texpr,
      std::vector<Expr*>* exprs);

  /// Create a new literal expr of 'type' with initial 'data'.
  /// data should match the ColumnType (i.e. type == TYPE_INT, data is a int*)
  /// The new Expr will be allocated from the pool.
  static Expr* CreateLiteral(ObjectPool* pool, const ColumnType& type, void* data);

  /// Create a new literal expr of 'type' by parsing the string.
  /// NULL will be returned if the string and type are not compatible.
  /// The new Expr will be allocated from the pool.
  static Expr* CreateLiteral(ObjectPool* pool, const ColumnType& type,
      const std::string&);

  /// Computes a memory efficient layout for storing the results of evaluating
  /// 'exprs'. The results are assumed to be void* slot types (vs AnyVal types). Varlen
  /// data is not included (e.g. there will be space for a StringValue, but not the data
  /// referenced by it).
  ///
  /// Returns the number of bytes necessary to store all the results and offsets
  /// where the result for each expr should be stored.
  ///
  /// Variable length types are guaranteed to be at the end and 'var_result_begin'
  /// will be set the beginning byte offset where variable length results begin.
  /// 'var_result_begin' will be set to -1 if there are no variable len types.
  static int ComputeResultsLayout(const std::vector<Expr*>& exprs,
      std::vector<int>* offsets, int* var_result_begin);
  static int ComputeResultsLayout(const std::vector<ExprContext*>& ctxs,
      std::vector<int>* offsets, int* var_result_begin);

  /// Returns an llvm::Function* with signature:
  /// <subclass of AnyVal> ComputeFn(ExprContext* context, const TupleRow* row)
  //
  /// The function should evaluate this expr over 'row' and return the result as the
  /// appropriate type of AnyVal.
  virtual Status GetCodegendComputeFn(LlvmCodeGen* codegen, llvm::Function** fn) = 0;

  /// If this expr is constant, evaluates the expr with no input row argument and returns
  /// the result in 'const_val'. Sets 'const_val' to NULL if the argument is not constant.
  /// The returned AnyVal and associated varlen data is owned by 'context'. This should
  /// only be called after Open() has been called on this expr. Returns an error if there
  /// was an error evaluating the expression or if memory could not be allocated for the
  /// expression result.
  virtual Status GetConstVal(
      RuntimeState* state, ExprContext* context, AnyVal** const_val);

  virtual std::string DebugString() const;
  static std::string DebugString(const std::vector<Expr*>& exprs);

  /// The builtin functions are not called from anywhere in the code and the
  /// symbols are therefore not included in the binary. We call these functions
  /// by using dlsym. The compiler must think this function is callable to
  /// not strip these symbols.
  static void InitBuiltinsDummy();

  /// Any additions to this enum must be reflected in both GetConstant*() and
  /// GetIrConstant().
  enum ExprConstant {
    RETURN_TYPE_SIZE, // int
    RETURN_TYPE_PRECISION, // int
    RETURN_TYPE_SCALE, // int
    ARG_TYPE_SIZE, // int[]
    ARG_TYPE_PRECISION, // int[]
    ARG_TYPE_SCALE, // int[]
  };

  /// Static function for obtaining a runtime constant.  Expr compute functions and
  /// builtins implementing the UDF interface should use this function, rather than
  /// accessing runtime constants directly, so any recognized constants can be inlined
  /// via InlineConstants() in the codegen path. In the interpreted path, this function
  /// will work as-is.
  ///
  /// 'c' determines which constant is returned. The type of the constant is annotated in
  /// the ExprConstant enum above. If the constant is an array, 'i' must be specified and
  /// indicates which element to return. 'i' must always be an immediate integer value so
  /// InlineConstants() can resolve the index, e.g., it cannot be a variable or an
  /// expression like "1 + 1".  For example, if 'c' = ARG_TYPE_SIZE, then 'T' = int and
  /// 0 <= i < children_.size().
  ///
  /// InlineConstants() can be run on the function to replace recognized constants. The
  /// constants are only replaced in the function itself, so any callee functions with
  /// constants to be replaced must be inlined into the function that InlineConstants()
  /// is run on (e.g. by annotating them with IR_ALWAYS_INLINE).
  ///
  /// TODO: implement a loop unroller (or use LLVM's) so we can use GetConstantInt()
  /// in loops
  static int GetConstantInt(const FunctionContext& ctx, ExprConstant c, int i = -1);

  /// Finds all calls to Expr::GetConstantInt() in 'fn' and replaces them with the
  /// appropriate runtime constants based on the arguments. 'return_type' is the
  /// return type of the UDF or UDAF, i.e. the value of FunctionContext::GetReturnType().
  /// 'arg_types' are the argument types of the UDF or UDAF, i.e. the values of
  /// FunctionContext::GetArgType().
  static int InlineConstants(const FunctionContext::TypeDesc& return_type,
      const std::vector<FunctionContext::TypeDesc>& arg_types,
      LlvmCodeGen* codegen, llvm::Function* fn);

  static const char* LLVM_CLASS_NAME;

  /// Expr::GetConstantInt() symbol prefix.
  static const char* GET_CONSTANT_INT_SYMBOL_PREFIX;

 protected:
  friend class AggFnEvaluator;
  friend class CastExpr;
  friend class ComputeFunctions;
  friend class DecimalFunctions;
  friend class DecimalLliteral;
  friend class DecimalOperators;
  friend class MathFunctions;
  friend class StringFunctions;
  friend class TimestampFunctions;
  friend class ConditionalFunctions;
  friend class UtilityFunctions;
  friend class CaseExpr;
  friend class InPredicate;
  friend class FunctionCall;
  friend class ScalarFnCall;

  Expr(const ColumnType& type, bool is_slotref = false, int fn_context_index = -1);
  Expr(const TExprNode& node, bool is_slotref = false, int fn_context_index = -1);

  /// Initializes this expr instance for execution.
  /// Subclasses overriding this function should call Expr::Init() to recursively call
  /// Init() on the expr tree.
  virtual Status Init(RuntimeState* state, const RowDescriptor& row_desc);

  /// Initializes 'context' for execution. If scope if FRAGMENT_LOCAL, both fragment- and
  /// thread-local state should be initialized. Otherwise, if scope is THREAD_LOCAL, only
  /// thread-local state should be initialized.
  ///
  /// Subclasses overriding this function should call Expr::OpenContext() to recursively
  /// call OpenContext() on all Expr nodes in the expr tree.
  virtual Status OpenContext(RuntimeState* state, ExprContext* context,
      FunctionContext::FunctionStateScope scope = FunctionContext::FRAGMENT_LOCAL);

  /// Subclasses overriding this function should call Expr::CloseContext() on the Expr
  /// nodes in the expr tree.
  ///
  /// If scope if FRAGMENT_LOCAL, both fragment- and thread-local state should be torn
  /// down. Otherwise, if scope is THREAD_LOCAL, only thread-local state should be torn
  /// down.
  virtual void CloseContext(RuntimeState* state, ExprContext* context,
      FunctionContext::FunctionStateScope scope = FunctionContext::FRAGMENT_LOCAL);

  /// Computes the size of the varargs buffer in bytes (0 bytes if no varargs).
  virtual int ComputeVarArgsBufferSize() const { return 0; }

  /// Cache entry for the library implementing this function.
  LibCacheEntry* cache_entry_;

  /// Function description.
  TFunction fn_;

  /// recognize if this node is a slotref in order to speed up GetValue()
  const bool is_slotref_;

  /// Index to pass to ExprContext::fn_context() to retrieve this expr's FunctionContext.
  /// Set when creating the Expr tree. -1 if this expr does not need a FunctionContext.
  const int fn_context_index_;

  /// ['fn_context_index_start_', 'fn_context_index_end_') is the range of index
  /// of the FunctionContext belonging to this expression tree (i.e. this Expr and
  /// all its descendants). It's passed to ExprContext::GetFnContextError() to check
  /// if there is any error during expression evaluation. Note that even if this expr
  /// does not have any FunctionContext (i.e. fn_context_index_ == -1), it will have
  /// a non-empty range if its descendants have any FunctionContexts.
  int fn_context_index_start_;
  int fn_context_index_end_;

  /// The type of this Expr.
  const ColumnType type_;

  std::vector<Expr*> children_;

  /// Cached codegened compute function. Exprs should set this in GetCodegendComputeFn().
  llvm::Function* ir_compute_fn_;

  /// Helper function to create an empty Function* with the appropriate signature to be
  /// returned by GetCodegendComputeFn(). 'name' is the name of the returned Function*.
  /// The arguments to the function are returned in 'args'.
  llvm::Function* CreateIrFunctionPrototype(LlvmCodeGen* codegen, const std::string& name,
      llvm::Value* (*args)[2]);

  /// Generates an IR compute function that calls the appropriate interpreted Get*Val()
  /// compute function.
  //
  /// This is useful for builtins that can't be implemented with the UDF interface
  /// (e.g. functions that need short-circuiting) and that don't have custom codegen
  /// functions that use the IRBuilder. It doesn't provide any performance benefit over
  /// the interpreted path.
  /// TODO: this should be replaced with fancier xcompiling infrastructure
  Status GetCodegendComputeFnWrapper(LlvmCodeGen* codegen, llvm::Function** fn);

  /// Returns the IR version of the static Get*Val() wrapper function corresponding to
  /// 'type'. This is used for calling interpreted Get*Val() functions from codegen'd
  /// functions (e.g. in ScalarFnCall() when codegen is disabled).
  llvm::Function* GetStaticGetValWrapper(ColumnType type, LlvmCodeGen* codegen);

  /// Replace all calls to Expr::GetConstant() in 'fn' based on the types of the
  /// expr and its children. This is a convenience method that invokes the static
  /// InlineConstants() function with the correct arguments for the expr.
  int InlineConstants(LlvmCodeGen* codegen, llvm::Function* fn);

  /// Simple debug string that provides no expr subclass-specific information
  std::string DebugString(const std::string& expr_name) const;

 private:
  friend class ExprContext;
  friend class ExprTest;
  friend class ExprCodegenTest;

  /// Create a new Expr based on texpr_node.node_type within 'pool'.
  /// 'fn_context_index' is a pointer to the next available index for FunctionContext.
  /// An Expr in the tree assigns 'fn_context_index' to itself and bumps it by 1 if the
  /// it needs a FunctionContext.
  static Status CreateExpr(ObjectPool* pool, const TExprNode& texpr_node,
      Expr** expr, int* fn_context_index);

  /// Create expression tree from the vector of nodes contained in 'texpr' within 'pool'.
  /// Returns the root of expression tree in 'root'. 'node_idx' is the starting index
  /// into the vector of nodes which this function should start constructing the tree.
  /// It's updated as the TExprNode is consumed for constructing the expression tree.
  /// 'use_all_nodes' is true iff all ExprNode in 'texpr' should be consumed to construct
  /// the expression tree. An error status is returned if 'use_all_nodes' is true but
  /// not all nodes are consumed in constructing the expression tree.
  static Status CreateExprTree(ObjectPool* pool, const TExpr& texpr, Expr** root,
      int* node_idx, bool use_all_nodes);

  /// Creates an expr tree for the node rooted at 'node_idx' via depth-first traversal.
  /// parameters
  ///   pool: Object pool in which Expr created from nodes are stored
  ///   nodes: vector of thrift expression nodes to be translated
  ///   parent: parent of node at node_idx (or NULL for node_idx == 0)
  ///   node_idx:
  ///     in: root of TExprNode tree
  ///     out: next node in 'nodes' that isn't part of tree
  ///   root_expr: out: root of constructed expr tree
  ///   fn_context_index: pointer to the next available index for FunctionContext
  /// return
  ///   status.ok() if successful
  ///   !status.ok() if tree is inconsistent or corrupt
  static Status CreateTreeFromThrift(ObjectPool* pool,
      const std::vector<TExprNode>& nodes, Expr* parent, int* node_idx,
      Expr** root_expr, int* fn_context_index);

  /// Static wrappers around the virtual Get*Val() functions. Calls the appropriate
  /// Get*Val() function on expr, passing it the context and row arguments.
  //
  /// These are used to call Get*Val() functions from generated functions, since I don't
  /// know how to call virtual functions directly. GetStaticGetValWrapper() returns the
  /// IR function of the appropriate wrapper function.
  static BooleanVal GetBooleanVal(Expr* expr, ExprContext* context, const TupleRow* row);
  static TinyIntVal GetTinyIntVal(Expr* expr, ExprContext* context, const TupleRow* row);
  static SmallIntVal GetSmallIntVal(Expr* expr, ExprContext* context, const TupleRow* row);
  static IntVal GetIntVal(Expr* expr, ExprContext* context, const TupleRow* row);
  static BigIntVal GetBigIntVal(Expr* expr, ExprContext* context, const TupleRow* row);
  static FloatVal GetFloatVal(Expr* expr, ExprContext* context, const TupleRow* row);
  static DoubleVal GetDoubleVal(Expr* expr, ExprContext* context, const TupleRow* row);
  static StringVal GetStringVal(Expr* expr, ExprContext* context, const TupleRow* row);
  static TimestampVal GetTimestampVal(Expr* expr, ExprContext* context, const TupleRow* row);
  static DecimalVal GetDecimalVal(Expr* expr, ExprContext* context, const TupleRow* row);

  // Helper function for GetConstantInt() and InlineConstants(): return the constant value
  // given the specific argument and return types.
  static int GetConstantInt(const FunctionContext::TypeDesc& return_type,
      const std::vector<FunctionContext::TypeDesc>& arg_types, ExprConstant c,
      int i = -1);
};

}

#endif
