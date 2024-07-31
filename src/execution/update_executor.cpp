//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}
// As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.

void UpdateExecutor::Init() {
  table_id_ = plan_->TableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_id_);
  index_list_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  has_out_ = false;
  child_executor_->Init();
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (has_out_) {
    return false;
  }
  int nums = 0;
  while (child_executor_->Next(tuple, rid)) {
    std::vector<Value> values{};
    for (auto &express : plan_->target_expressions_) {
      values.push_back(express->Evaluate(tuple, table_info_->schema_));
    }
    Tuple new_tuple = Tuple(values, &table_info_->schema_);
    auto new_tuplemeta = TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false};
    table_info_->table_->UpdateTupleInPlaceUnsafe(new_tuplemeta, new_tuple, *rid);
    nums++;

    for (auto &x : index_list_) {
      Tuple partial_tuple =
          tuple->KeyFromTuple(table_info_->schema_, *(x->index_->GetKeySchema()), x->index_->GetKeyAttrs());
      x->index_->DeleteEntry(partial_tuple, *rid, exec_ctx_->GetTransaction());

      Tuple partial_new_tuple =
          new_tuple.KeyFromTuple(table_info_->schema_, *(x->index_->GetKeySchema()), x->index_->GetKeyAttrs());
      x->index_->InsertEntry(partial_new_tuple, *rid, exec_ctx_->GetTransaction());
    }
  }

  std::vector<Value> values{};
  values.emplace_back(Value(INTEGER, nums));
  *tuple = Tuple(values, &GetOutputSchema());

  has_out_ = true;

  return true;
}

}  // namespace bustub
