//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  table_id_ = plan_->TableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(table_id_);
  index_list_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                              table_info_->oid_)) {
    throw ExecutionException("lock table intention exclusive failed");
  }
  child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple produce_tuple;
  RID produce_rid;
  int count = 0;
  while (true) {
    if (child_executor_ == nullptr) {
      return false;
    }
    bool status = child_executor_->Next(&produce_tuple, &produce_rid);
    if (!status) {
      break;
    }
    TupleMeta tuple_meta = table_info_->table_->GetTupleMeta(produce_rid);
    tuple_meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(tuple_meta, produce_rid);
    count++;
    for (auto &index_info : index_list_) {
      if (index_info != nullptr) {
        index_info->index_->DeleteEntry(produce_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                                                   index_info->index_->GetKeyAttrs()),
                                        produce_rid, exec_ctx_->GetTransaction());
      }
    }
  }
  child_executor_ = nullptr;
  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, count);
  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}

}  // namespace bustub
