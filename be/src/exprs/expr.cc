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

#include <sstream>

#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/IR/InstIterator.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Utils/BasicBlockUtils.h>
#include <llvm/Transforms/Utils/UnrollLoop.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "exprs/aggregate-functions.h"
#include "exprs/anyval-util.h"
#include "exprs/bit-byte-functions.h"
#include "exprs/case-expr.h"
#include "exprs/cast-functions.h"
#include "exprs/compound-predicates.h"
#include "exprs/conditional-functions.h"
#include "exprs/decimal-functions.h"
#include "exprs/decimal-operators.h"
#include "exprs/expr-context.h"
#include "exprs/expr.h"
#include "exprs/hive-udf-call.h"
#include "exprs/in-predicate.h"
#include "exprs/is-not-empty-predicate.h"
#include "exprs/is-null-predicate.h"
#include "exprs/like-predicate.h"
#include "exprs/literal.h"
#include "exprs/math-functions.h"
#include "exprs/null-literal.h"
#include "exprs/operators.h"
#include "exprs/scalar-fn-call.h"
#include "exprs/slot-ref.h"
#include "exprs/string-functions.h"
#include "exprs/timestamp-functions.h"
#include "exprs/tuple-is-null-predicate.h"
#include "exprs/udf-builtins.h"
#include "exprs/utility-functions.h"
#include "gen-cpp/Data_types.h"
#include "gen-cpp/Exprs_types.h"
#include "runtime/lib-cache.h"
#include "runtime/mem-tracker.h"
#include "runtime/raw-value.h"
#include "runtime/runtime-state.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "udf/udf-internal.h"
#include "udf/udf.h"

#include "gen-cpp/Exprs_types.h"
#include "gen-cpp/ImpalaService_types.h"

#include "common/names.h"

using namespace impala_udf;
using namespace llvm;

namespace impala {

const char* Expr::LLVM_CLASS_NAME = "class.impala::Expr";

const char* Expr::GET_CONSTANT_INT_SYMBOL_PREFIX = "_ZN6impala4Expr14GetConstantInt";

template<class T>
bool ParseString(const string& str, T* val) {
  istringstream stream(str);
  stream >> *val;
  return !stream.fail();
}

Expr::Expr(const ColumnType& type, bool is_slotref, int fn_context_index)
    : cache_entry_(NULL),
      is_slotref_(is_slotref),
      fn_context_index_(fn_context_index),
      fn_context_index_start_(-1),
      fn_context_index_end_(-1),
      type_(type),
      ir_compute_fn_(NULL) {
}

Expr::Expr(const TExprNode& node, bool is_slotref, int fn_context_index)
    : cache_entry_(NULL),
      is_slotref_(is_slotref),
      fn_context_index_(fn_context_index),
      fn_context_index_start_(-1),
      fn_context_index_end_(-1),
      type_(ColumnType::FromThrift(node.type)),
      ir_compute_fn_(NULL) {
  if (node.__isset.fn) fn_ = node.fn;
}

Expr::~Expr() {
  if (cache_entry_ != NULL) {
    LibCache::instance()->DecrementUseCount(cache_entry_);
    cache_entry_ = NULL;
  }
}

void Expr::CloseContext(RuntimeState* state, ExprContext* context,
    FunctionContext::FunctionStateScope scope) {
  for (Expr* child : children_) child->CloseContext(state, context, scope);
}

Status Expr::CreateExprTree(ObjectPool* pool, const TExpr& texpr, Expr** root,
    int* node_idx, bool use_all_nodes) {
  int fn_context_index = 0;
  Status status =
      CreateTreeFromThrift(pool, texpr.nodes, NULL, node_idx, root, &fn_context_index);
  if (status.ok()) {
    if (use_all_nodes && *node_idx + 1 != texpr.nodes.size()) {
      return Status("Expression tree only partially reconstructed. Not all thrift " \
          "nodes were used.");
    }
  } else {
    LOG(ERROR) << "Could not construct expr tree.\n" << status.GetDetail() << "\n"
               << apache::thrift::ThriftDebugString(texpr);
  }
  return status;
}

Status Expr::CreateExprTree(ObjectPool* pool, const TExpr& texpr, Expr** expr) {
  // input is empty
  if (texpr.nodes.size() == 0) {
    *expr = NULL;
    return Status::OK();
  }
  int node_idx = 0;
  return CreateExprTree(pool, texpr, expr, &node_idx, true);
}

Status Expr::CreateExprTrees(ObjectPool* pool, const vector<TExpr>& texprs,
    vector<Expr*>* exprs) {
  exprs->clear();
  for (const TExpr& texpr: texprs) {
    Expr* expr;
    RETURN_IF_ERROR(CreateExprTree(pool, texpr, &expr));
    exprs->push_back(expr);
  }
  return Status::OK();
}

Status Expr::CreateInputExprTrees(ObjectPool* pool, const TExpr& texpr,
    vector<Expr*>* exprs) {
  DCHECK(texpr.nodes[0].node_type == TExprNodeType::AGGREGATE_EXPR ||
      texpr.nodes[0].node_type == TExprNodeType::FUNCTION_CALL);
  int node_idx = 1;
  int num_inputs = texpr.nodes[0].num_children;
  for (int i = 0; i < num_inputs; ++i) {
    DCHECK_LT(node_idx, texpr.nodes.size());
    Expr* expr;
    RETURN_IF_ERROR(CreateExprTree(pool, texpr, &expr, &node_idx, i == num_inputs - 1));
    exprs->push_back(expr);
    ++node_idx;
  }
  return Status::OK();
}

Status Expr::CreateTreeFromThrift(ObjectPool* pool, const vector<TExprNode>& nodes,
    Expr* parent, int* node_idx, Expr** root_expr, int* fn_context_index) {
  // propagate error case
  if (*node_idx >= nodes.size()) {
    return Status("Failed to reconstruct expression tree from thrift.");
  }
  int num_children = nodes[*node_idx].num_children;
  Expr* expr = NULL;
  // Save 'fn_context_index' before creating the expression.
  int fn_context_index_start = *fn_context_index;
  RETURN_IF_ERROR(CreateExpr(pool, nodes[*node_idx], &expr, fn_context_index));
  DCHECK(expr != NULL);
  if (parent != NULL) {
    parent->AddChild(expr);
  } else {
    DCHECK(root_expr != NULL);
    *root_expr = expr;
  }
  for (int i = 0; i < num_children; i++) {
    *node_idx += 1;
    RETURN_IF_ERROR(
        CreateTreeFromThrift(pool, nodes, expr, node_idx, NULL, fn_context_index));
    // we are expecting a child, but have used all nodes
    // this means we have been given a bad tree and must fail
    if (*node_idx >= nodes.size()) {
      return Status("Failed to reconstruct expression tree from thrift.");
    }
  }
  expr->fn_context_index_start_ = fn_context_index_start;
  expr->fn_context_index_end_ = *fn_context_index;
  return Status::OK();
}

Status Expr::CreateExpr(ObjectPool* pool, const TExprNode& texpr_node, Expr** expr,
    int* fn_context_index) {
  switch (texpr_node.node_type) {
    case TExprNodeType::BOOL_LITERAL:
    case TExprNodeType::FLOAT_LITERAL:
    case TExprNodeType::INT_LITERAL:
    case TExprNodeType::STRING_LITERAL:
    case TExprNodeType::DECIMAL_LITERAL:
    case TExprNodeType::TIMESTAMP_LITERAL:
      *expr = pool->Add(new Literal(texpr_node));
      return Status::OK();
    case TExprNodeType::CASE_EXPR:
      if (!texpr_node.__isset.case_expr) {
        return Status("Case expression not set in thrift node");
      }
      *expr = pool->Add(new CaseExpr(texpr_node, *fn_context_index));
      ++(*fn_context_index);
      return Status::OK();
    case TExprNodeType::COMPOUND_PRED:
      if (texpr_node.fn.name.function_name == "and") {
        *expr = pool->Add(new AndPredicate(texpr_node));
      } else if (texpr_node.fn.name.function_name == "or") {
        *expr = pool->Add(new OrPredicate(texpr_node));
      } else {
        DCHECK_EQ(texpr_node.fn.name.function_name, "not");
        *expr = pool->Add(new ScalarFnCall(texpr_node, *fn_context_index));
        ++(*fn_context_index);
      }
      return Status::OK();
    case TExprNodeType::NULL_LITERAL:
      *expr = pool->Add(new NullLiteral(texpr_node));
      return Status::OK();
    case TExprNodeType::SLOT_REF:
      if (!texpr_node.__isset.slot_ref) {
        return Status("Slot reference not set in thrift node");
      }
      *expr = pool->Add(new SlotRef(texpr_node));
      return Status::OK();
    case TExprNodeType::TUPLE_IS_NULL_PRED:
      *expr = pool->Add(new TupleIsNullPredicate(texpr_node));
      return Status::OK();
    case TExprNodeType::FUNCTION_CALL:
      if (!texpr_node.__isset.fn) {
        return Status("Function not set in thrift node");
      }
      // Special-case functions that have their own Expr classes
      // TODO: is there a better way to do this?
      if (texpr_node.fn.name.function_name == "if") {
        *expr = pool->Add(new IfExpr(texpr_node));
      } else if (texpr_node.fn.name.function_name == "nullif") {
        *expr = pool->Add(new NullIfExpr(texpr_node));
      } else if (texpr_node.fn.name.function_name == "isnull" ||
                 texpr_node.fn.name.function_name == "ifnull" ||
                 texpr_node.fn.name.function_name == "nvl") {
        *expr = pool->Add(new IsNullExpr(texpr_node));
      } else if (texpr_node.fn.name.function_name == "coalesce") {
        *expr = pool->Add(new CoalesceExpr(texpr_node));
      } else if (texpr_node.fn.binary_type == TFunctionBinaryType::JAVA) {
        *expr = pool->Add(new HiveUdfCall(texpr_node, *fn_context_index));
        ++(*fn_context_index);
      } else {
        *expr = pool->Add(new ScalarFnCall(texpr_node, *fn_context_index));
        ++(*fn_context_index);
      }
      return Status::OK();
    case TExprNodeType::IS_NOT_EMPTY_PRED:
      *expr = pool->Add(new IsNotEmptyPredicate(texpr_node));
      return Status::OK();
    default:
      stringstream os;
      os << "Unknown expr node type: " << texpr_node.node_type;
      return Status(os.str());
  }
}

struct MemLayoutData {
  int expr_idx;
  int byte_size;
  bool variable_length;
  int alignment;

  // TODO: sort by type as well?  Any reason to do this?
  // TODO: would sorting in reverse order of size be faster due to better packing?
  // TODO: why put var-len at end?
  bool operator<(const MemLayoutData& rhs) const {
    // variable_len go at end
    if (this->variable_length && !rhs.variable_length) return false;
    if (!this->variable_length && rhs.variable_length) return true;
    return this->byte_size < rhs.byte_size;
  }
};

int Expr::ComputeResultsLayout(const vector<Expr*>& exprs, vector<int>* offsets,
    int* var_result_begin) {
  if (exprs.size() == 0) {
    *var_result_begin = -1;
    return 0;
  }

  // Don't align more than word (8-byte) size. There's no performance gain beyond 8-byte
  // alignment, and there is a performance gain to keeping the results buffer small. This
  // is consistent with what compilers do.
  int MAX_ALIGNMENT = sizeof(int64_t);

  vector<MemLayoutData> data;
  data.resize(exprs.size());

  // Collect all the byte sizes and sort them
  for (int i = 0; i < exprs.size(); ++i) {
    DCHECK(!exprs[i]->type().IsComplexType()) << "NYI";
    data[i].expr_idx = i;
    data[i].byte_size = exprs[i]->type().GetSlotSize();
    DCHECK_GT(data[i].byte_size, 0);
    data[i].variable_length = exprs[i]->type().IsVarLenStringType();

    bool fixed_len_char = exprs[i]->type().type == TYPE_CHAR && !data[i].variable_length;

    // Compute the alignment of this value. Values should be self-aligned for optimal
    // memory access speed, up to the max alignment (e.g., if this value is an int32_t,
    // its offset in the buffer should be divisible by sizeof(int32_t)).
    // TODO: is self-alignment really necessary for perf?
    if (!fixed_len_char) {
      data[i].alignment = min(data[i].byte_size, MAX_ALIGNMENT);
    } else {
      // Fixed-len chars are aligned to a one-byte boundary, as if they were char[],
      // leaving no padding between them and the previous value.
      data[i].alignment = 1;
    }
  }

  sort(data.begin(), data.end());

  // Walk the types and store in a packed aligned layout
  int byte_offset = 0;

  offsets->resize(exprs.size());
  *var_result_begin = -1;

  for (int i = 0; i < data.size(); ++i) {
    // Increase byte_offset so data[i] is at the right alignment (i.e. add padding between
    // this value and the previous).
    byte_offset = BitUtil::RoundUp(byte_offset, data[i].alignment);

    (*offsets)[data[i].expr_idx] = byte_offset;
    if (data[i].variable_length && *var_result_begin == -1) {
      *var_result_begin = byte_offset;
    }
    DCHECK(!(i == 0 && byte_offset > 0)) << "first value should be at start of layout";
    byte_offset += data[i].byte_size;
  }

  return byte_offset;
}

int Expr::ComputeResultsLayout(const vector<ExprContext*>& ctxs, vector<int>* offsets,
    int* var_result_begin) {
  vector<Expr*> exprs;
  for (int i = 0; i < ctxs.size(); ++i) exprs.push_back(ctxs[i]->root());
  return ComputeResultsLayout(exprs, offsets, var_result_begin);
}

Status Expr::Init(RuntimeState* state, const RowDescriptor& row_desc) {
  DCHECK(type_.type != INVALID_TYPE);
  for (int i = 0; i < children_.size(); ++i) {
    RETURN_IF_ERROR(children_[i]->Init(state, row_desc));
  }
  return Status::OK();
}

Status Expr::OpenContext(RuntimeState* state, ExprContext* context,
    FunctionContext::FunctionStateScope scope) {
  for (int i = 0; i < children_.size(); ++i) {
    RETURN_IF_ERROR(children_[i]->OpenContext(state, context, scope));
  }
  return Status::OK();
}

string Expr::DebugString() const {
  // TODO: implement partial debug string for member vars
  stringstream out;
  out << " type=" << type_.DebugString();
  if (!children_.empty()) {
    out << " children=" << DebugString(children_);
  }
  return out.str();
}

string Expr::DebugString(const vector<Expr*>& exprs) {
  stringstream out;
  out << "[";
  for (int i = 0; i < exprs.size(); ++i) {
    out << (i == 0 ? "" : " ") << exprs[i]->DebugString();
  }
  out << "]";
  return out.str();
}

bool Expr::IsConstant() const {
  for (int i = 0; i < children_.size(); ++i) {
    if (!children_[i]->IsConstant()) return false;
  }
  return true;
}

bool Expr::IsLiteral() const {
  return false;
}

int Expr::GetSlotIds(vector<SlotId>* slot_ids) const {
  int n = 0;
  for (int i = 0; i < children_.size(); ++i) {
    n += children_[i]->GetSlotIds(slot_ids);
  }
  return n;
}

Function* Expr::GetStaticGetValWrapper(ColumnType type, LlvmCodeGen* codegen) {
  switch (type.type) {
    case TYPE_BOOLEAN:
      return codegen->GetFunction(IRFunction::EXPR_GET_BOOLEAN_VAL, false);
    case TYPE_TINYINT:
      return codegen->GetFunction(IRFunction::EXPR_GET_TINYINT_VAL, false);
    case TYPE_SMALLINT:
      return codegen->GetFunction(IRFunction::EXPR_GET_SMALLINT_VAL, false);
    case TYPE_INT:
      return codegen->GetFunction(IRFunction::EXPR_GET_INT_VAL, false);
    case TYPE_BIGINT:
      return codegen->GetFunction(IRFunction::EXPR_GET_BIGINT_VAL, false);
    case TYPE_FLOAT:
      return codegen->GetFunction(IRFunction::EXPR_GET_FLOAT_VAL, false);
    case TYPE_DOUBLE:
      return codegen->GetFunction(IRFunction::EXPR_GET_DOUBLE_VAL, false);
    case TYPE_STRING:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
      return codegen->GetFunction(IRFunction::EXPR_GET_STRING_VAL, false);
    case TYPE_TIMESTAMP:
      return codegen->GetFunction(IRFunction::EXPR_GET_TIMESTAMP_VAL, false);
    case TYPE_DECIMAL:
      return codegen->GetFunction(IRFunction::EXPR_GET_DECIMAL_VAL, false);
    default:
      DCHECK(false) << "Invalid type: " << type.DebugString();
      return NULL;
  }
}

Function* Expr::CreateIrFunctionPrototype(LlvmCodeGen* codegen, const string& name,
                                          Value* (*args)[2]) {
  Type* return_type = CodegenAnyVal::GetLoweredType(codegen, type());
  LlvmCodeGen::FnPrototype prototype(codegen, name, return_type);
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable(
          "context", codegen->GetPtrType(ExprContext::LLVM_CLASS_NAME)));
  prototype.AddArgument(
      LlvmCodeGen::NamedVariable("row", codegen->GetPtrType(TupleRow::LLVM_CLASS_NAME)));
  Function* function = prototype.GeneratePrototype(NULL, args[0]);
  DCHECK(function != NULL);
  return function;
}

void Expr::InitBuiltinsDummy() {
  // Call one function from each of the classes to pull all the symbols
  // from that class in.
  // TODO: is there a better way to do this?
  AggregateFunctions::InitNull(NULL, NULL);
  BitByteFunctions::CountSet(NULL, TinyIntVal::null());
  CastFunctions::CastToBooleanVal(NULL, TinyIntVal::null());
  CompoundPredicate::Not(NULL, BooleanVal::null());
  ConditionalFunctions::NullIfZero(NULL, TinyIntVal::null());
  DecimalFunctions::Precision(NULL, DecimalVal::null());
  DecimalOperators::CastToDecimalVal(NULL, DecimalVal::null());
  InPredicate::InIterate(NULL, BigIntVal::null(), 0, NULL);
  IsNullPredicate::IsNull(NULL, BooleanVal::null());
  LikePredicate::Like(NULL, StringVal::null(), StringVal::null());
  Operators::Add_IntVal_IntVal(NULL, IntVal::null(), IntVal::null());
  MathFunctions::Pi(NULL);
  StringFunctions::Length(NULL, StringVal::null());
  TimestampFunctions::Year(NULL, TimestampVal::null());
  TimestampFunctions::UnixAndFromUnixPrepare(NULL, FunctionContext::FRAGMENT_LOCAL);
  UdfBuiltins::Pi(NULL);
  UtilityFunctions::Pid(NULL);
}

Status Expr::GetConstVal(RuntimeState* state, ExprContext* context, AnyVal** const_val) {
  DCHECK(context->opened_);
  if (!IsConstant()) {
    *const_val = NULL;
    return Status::OK();
  }

  RETURN_IF_ERROR(AllocateAnyVal(state, context->pool_.get(), type_,
      "Could not allocate constant expression value", const_val));
  switch (type_.type) {
    case TYPE_BOOLEAN:
      *reinterpret_cast<BooleanVal*>(*const_val) = GetBooleanVal(context, NULL);
      break;
    case TYPE_TINYINT:
      *reinterpret_cast<TinyIntVal*>(*const_val) = GetTinyIntVal(context, NULL);
      break;
    case TYPE_SMALLINT:
      *reinterpret_cast<SmallIntVal*>(*const_val) = GetSmallIntVal(context, NULL);
      break;
    case TYPE_INT:
      *reinterpret_cast<IntVal*>(*const_val) = GetIntVal(context, NULL);
      break;
    case TYPE_BIGINT:
      *reinterpret_cast<BigIntVal*>(*const_val) = GetBigIntVal(context, NULL);
      break;
    case TYPE_FLOAT:
      *reinterpret_cast<FloatVal*>(*const_val) = GetFloatVal(context, NULL);
      break;
    case TYPE_DOUBLE:
      *reinterpret_cast<DoubleVal*>(*const_val) = GetDoubleVal(context, NULL);
      break;
    case TYPE_STRING:
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
      StringVal* sv = reinterpret_cast<StringVal*>(*const_val);
      *sv = GetStringVal(context, NULL);
      if (sv->len > 0) {
        // Make sure the memory is owned by 'context'.
        uint8_t* ptr_copy = context->pool_->TryAllocate(sv->len);
        if (ptr_copy == NULL) {
          return context->pool_->mem_tracker()->MemLimitExceeded(
              state, "Could not allocate constant string value", sv->len);
        }
        memcpy(ptr_copy, sv->ptr, sv->len);
        sv->ptr = ptr_copy;
      }
      break;
    }
    case TYPE_TIMESTAMP:
      *reinterpret_cast<TimestampVal*>(*const_val) = GetTimestampVal(context, NULL);
      break;
    case TYPE_DECIMAL:
      *reinterpret_cast<DecimalVal*>(*const_val) = GetDecimalVal(context, NULL);
      break;
    default:
      DCHECK(false) << "Type not implemented: " << type();
  }
  // Errors may have been set during the GetConstVal() call.
  return context->GetFnContextError(fn_context_index_start_, fn_context_index_end_);
}

int Expr::GetConstantInt(const FunctionContext::TypeDesc& return_type,
    const std::vector<FunctionContext::TypeDesc>& arg_types, ExprConstant c, int i) {
  switch (c) {
    case RETURN_TYPE_SIZE:
      DCHECK_EQ(i, -1);
      return AnyValUtil::TypeDescToColumnType(return_type).GetByteSize();
    case RETURN_TYPE_PRECISION:
      DCHECK_EQ(i, -1);
      DCHECK_EQ(return_type.type, FunctionContext::TYPE_DECIMAL);
      return return_type.precision;
    case RETURN_TYPE_SCALE:
      DCHECK_EQ(i, -1);
      DCHECK_EQ(return_type.type, FunctionContext::TYPE_DECIMAL);
      return return_type.scale;
    case ARG_TYPE_SIZE:
      DCHECK_GE(i, 0);
      DCHECK_LT(i, arg_types.size());
      return AnyValUtil::TypeDescToColumnType(arg_types[i]).GetByteSize();
    case ARG_TYPE_PRECISION:
      DCHECK_GE(i, 0);
      DCHECK_LT(i, arg_types.size());
      DCHECK_EQ(arg_types[i].type, FunctionContext::TYPE_DECIMAL);
      return arg_types[i].precision;
    case ARG_TYPE_SCALE:
      DCHECK_GE(i, 0);
      DCHECK_LT(i, arg_types.size());
      DCHECK_EQ(arg_types[i].type, FunctionContext::TYPE_DECIMAL);
      return arg_types[i].scale;
    default:
      CHECK(false) << "NYI";
      return -1;
  }
}

int Expr::GetConstantInt(const FunctionContext& ctx, ExprConstant c, int i) {
  return GetConstantInt(ctx.GetReturnType(), ctx.impl()->arg_types(), c, i);
}

int Expr::InlineConstants(LlvmCodeGen* codegen, Function* fn) {
  FunctionContext::TypeDesc return_type = AnyValUtil::ColumnTypeToTypeDesc(type_);
  vector<FunctionContext::TypeDesc> arg_types;
  for (int i = 0; i < children_.size(); ++i) {
    arg_types.push_back(AnyValUtil::ColumnTypeToTypeDesc(children_[i]->type_));
  }
  return InlineConstants(return_type, arg_types, codegen, fn);
}

int Expr::InlineConstants(const FunctionContext::TypeDesc& return_type,
      const std::vector<FunctionContext::TypeDesc>& arg_types, LlvmCodeGen* codegen,
      Function* fn) {
  int replaced = 0;
  for (inst_iterator iter = inst_begin(fn), end = inst_end(fn); iter != end; ) {
    // Increment iter now so we don't mess it up modifying the instruction below
    Instruction* instr = &*(iter++);

    // Look for call instructions
    if (!isa<CallInst>(instr)) continue;
    CallInst* call_instr = cast<CallInst>(instr);
    Function* called_fn = call_instr->getCalledFunction();

    // Look for call to Expr::GetConstant*()
    if (called_fn == NULL ||
        called_fn->getName().find(GET_CONSTANT_INT_SYMBOL_PREFIX) == string::npos) {
      continue;
    }

    // 'c' and 'i' arguments must be constant
    ConstantInt* c_arg = dyn_cast<ConstantInt>(call_instr->getArgOperand(1));
    ConstantInt* i_arg = dyn_cast<ConstantInt>(call_instr->getArgOperand(2));
    DCHECK(c_arg != NULL) << "Non-constant 'c' argument to Expr::GetConstant*()";
    DCHECK(i_arg != NULL) << "Non-constant 'i' argument to Expr::GetConstant*()";

    // Replace the called function with the appropriate constant
    ExprConstant c_val = static_cast<ExprConstant>(c_arg->getSExtValue());
    int i_val = static_cast<int>(i_arg->getSExtValue());
    // All supported constants are currently integers.
    call_instr->replaceAllUsesWith(ConstantInt::get(codegen->GetType(TYPE_INT),
          GetConstantInt(return_type, arg_types, c_val, i_val)));
    call_instr->eraseFromParent();
    ++replaced;
  }
  return replaced;
}

Status Expr::GetCodegendComputeFnWrapper(LlvmCodeGen* codegen, Function** fn) {
  if (ir_compute_fn_ != NULL) {
    *fn = ir_compute_fn_;
    return Status::OK();
  }
  Function* static_getval_fn = GetStaticGetValWrapper(type(), codegen);

  // Call it passing this as the additional first argument.
  Value* args[2];
  ir_compute_fn_ = CreateIrFunctionPrototype(codegen, "CodegenComputeFnWrapper", &args);
  BasicBlock* entry_block =
      BasicBlock::Create(codegen->context(), "entry", ir_compute_fn_);
  LlvmBuilder builder(entry_block);
  Value* this_ptr =
      codegen->CastPtrToLlvmPtr(codegen->GetPtrType(Expr::LLVM_CLASS_NAME), this);
  Value* compute_fn_args[] = {this_ptr, args[0], args[1]};
  Value* ret = CodegenAnyVal::CreateCall(
      codegen, &builder, static_getval_fn, compute_fn_args, "ret");
  builder.CreateRet(ret);
  ir_compute_fn_ = codegen->FinalizeFunction(ir_compute_fn_);
  *fn = ir_compute_fn_;
  return Status::OK();
}

// At least one of these should always be subclassed.
BooleanVal Expr::GetBooleanVal(ExprContext* context, const TupleRow* row) {
  DCHECK(false) << DebugString();
  return BooleanVal::null();
}
TinyIntVal Expr::GetTinyIntVal(ExprContext* context, const TupleRow* row) {
  DCHECK(false) << DebugString();
  return TinyIntVal::null();
}
SmallIntVal Expr::GetSmallIntVal(ExprContext* context, const TupleRow* row) {
  DCHECK(false) << DebugString();
  return SmallIntVal::null();
}
IntVal Expr::GetIntVal(ExprContext* context, const TupleRow* row) {
  DCHECK(false) << DebugString();
  return IntVal::null();
}
BigIntVal Expr::GetBigIntVal(ExprContext* context, const TupleRow* row) {
  DCHECK(false) << DebugString();
  return BigIntVal::null();
}
FloatVal Expr::GetFloatVal(ExprContext* context, const TupleRow* row) {
  DCHECK(false) << DebugString();
  return FloatVal::null();
}
DoubleVal Expr::GetDoubleVal(ExprContext* context, const TupleRow* row) {
  DCHECK(false) << DebugString();
  return DoubleVal::null();
}
StringVal Expr::GetStringVal(ExprContext* context, const TupleRow* row) {
  DCHECK(false) << DebugString();
  return StringVal::null();
}
CollectionVal Expr::GetCollectionVal(ExprContext* context, const TupleRow* row) {
  DCHECK(false) << DebugString();
  return CollectionVal::null();
}
TimestampVal Expr::GetTimestampVal(ExprContext* context, const TupleRow* row) {
  DCHECK(false) << DebugString();
  return TimestampVal::null();
}
DecimalVal Expr::GetDecimalVal(ExprContext* context, const TupleRow* row) {
  DCHECK(false) << DebugString();
  return DecimalVal::null();
}

string Expr::DebugString(const string& expr_name) const {
  stringstream out;
  out << expr_name << "(" << Expr::DebugString() << ")";
  return out.str();
}

}
