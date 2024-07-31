//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_oid_t tid = plan_->GetTableOid();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(tid);
  if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
      !exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED,
                                              table_info_->oid_)) {
    throw ExecutionException("lock table share failed");
  }
  iterator_ = std::make_unique<TableIterator>(table_info_->table_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::pair<TupleMeta, Tuple> tup;
  while (!iterator_->IsEnd()) {
    if (tup.first.is_deleted_) {
      ++(*iterator_);
      continue;
    }
    if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED &&
        !exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                              exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->oid_,
                                              iterator_->GetRID())) {
      throw ExecutionException("lock row intention share failed");
    }
    tup = iterator_->GetTuple();
    *tuple = tup.second;
    *rid = iterator_->GetRID();
    if (!exec_ctx_->GetTransaction()->GetSharedRowLockSet()->empty()) {
      if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
          !exec_ctx_->GetLockManager()->UnlockRow(
              exec_ctx_->GetTransaction(), exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->oid_, iterator_->GetRID())) {
        throw ExecutionException("unlock row share failed");
      }
    }
    break;
  }
  if (iterator_->IsEnd()) {
    return false;
  }
  ++(*iterator_);
  return true;
}

}  // namespace bustub
