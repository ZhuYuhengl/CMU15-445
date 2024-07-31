#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  this->page_ = that.page_;
  this->bpm_ = that.bpm_;
  this->is_dirty_ = that.is_dirty_;
  that.page_ = nullptr;
  that.bpm_ = nullptr;
  that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
  if (bpm_ != nullptr && page_ != nullptr) {
    bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  }
  this->page_ = nullptr;
  this->bpm_ = nullptr;
  this->is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  this->Drop();
  this->page_ = that.page_;
  this->bpm_ = that.bpm_;
  this->is_dirty_ = that.is_dirty_;
  that.page_ = nullptr;
  that.bpm_ = nullptr;
  that.is_dirty_ = false;
  return *this;
}

BasicPageGuard::~BasicPageGuard() { this->Drop(); };  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { this->guard_ = std::move(that.guard_); }

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  this->Drop();
  this->guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
  }
  if (guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.is_dirty_);
  }
  guard_.page_ = nullptr;
  guard_.bpm_ = nullptr;
  guard_.is_dirty_ = false;
}

ReadPageGuard::~ReadPageGuard() { this->Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { this->guard_ = std::move(that.guard_); }

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  this->Drop();
  this->guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
  }
  if (guard_.bpm_ != nullptr && guard_.page_ != nullptr) {
    guard_.bpm_->UnpinPage(guard_.page_->GetPageId(), guard_.is_dirty_);
  }
  guard_.page_ = nullptr;
  guard_.bpm_ = nullptr;
  guard_.is_dirty_ = false;
}

WritePageGuard::~WritePageGuard() { this->Drop(); }  // NOLINT

}  // namespace bustub
