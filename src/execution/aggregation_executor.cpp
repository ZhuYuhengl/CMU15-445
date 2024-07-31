//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <fmt/core.h>
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), aht_(plan->aggregates_, plan->agg_types_), aht_iterator_(aht_.Begin()) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
  // for(auto i : plan_->agg_types_){
  //   fmt::print("The aggregation type is: {}\n", i);
  // }
  // for(auto i : plan_->GetGroupBys()){
  //   fmt::print("The GroupBys is: {}\n", i);
  // }
  // for(auto i : plan_->GetAggregates()){
  //   fmt::print("The Aggregates is: {}\n", i);
  // }
}

void AggregationExecutor::Init() {
  std::vector<AbstractExpressionRef> aggregates = plan_->GetAggregates();
  std::vector<AbstractExpressionRef> group_bys = plan_->GetGroupBys();
  std::vector<AggregationType> arr_types = plan_->GetAggregateTypes();
  Tuple produce_tuple;
  RID produce_rid;
  AggregateKey key;
  AggregateValue val;
  aht_.Clear();
  child_executor_->Init();
  while (child_executor_->Next(&produce_tuple, &produce_rid)) {
    for (auto &it : group_bys) {
      key.group_bys_.push_back(it->Evaluate(&produce_tuple, child_executor_->GetOutputSchema()));
    }
    for (auto &it : aggregates) {
      val.aggregates_.push_back(it->Evaluate(&produce_tuple, child_executor_->GetOutputSchema()));
    }
    // for(auto i : key.group_bys_){
    //   fmt::print("The key is: {}\n", i);
    // }
    // for(auto i : val.aggregates_){
    //   fmt::print("The val is: {}\n", i);
    // }
    aht_.InsertCombine(key, val);
    key.group_bys_.clear();
    val.aggregates_.clear();
  }
  if (aht_.Begin() == aht_.End() && group_bys.empty()) {
    key.group_bys_.clear();
    val.aggregates_.clear();
    val = aht_.GenerateInitialAggregateValue();
    aht_.Insert(key, val);
  }
  aht_iterator_ = aht_.Begin();
  // std::cout << "group_by_size " << group_bys.size() << std::endl;
  // std::cout << "aggregates " << aggregates.size() << std::endl;
  // printf("aht size %ld\n", aht_.ht_.size());
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ != aht_.End()) {
    AggregateKey key = aht_iterator_.Key();
    AggregateValue value = aht_iterator_.Val();
    std::vector<Value> values;
    values.reserve(key.group_bys_.size() + value.aggregates_.size());
    for (auto &it : key.group_bys_) {
      values.push_back(it);
    }
    for (auto &it : value.aggregates_) {
      values.push_back(it);
    }
    *tuple = Tuple(values, &GetOutputSchema());
    ++aht_iterator_;
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
